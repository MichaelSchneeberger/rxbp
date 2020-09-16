import threading
from abc import abstractmethod, ABC
from dataclasses import dataclass
from typing import List, Callable

import rx
from rx.disposable import Disposable

from rxbp.acknowledgement.acksubject import AckSubject
from rxbp.acknowledgement.continueack import ContinueAck, continue_ack
from rxbp.acknowledgement.operators.reduceack import reduce_ack
from rxbp.acknowledgement.single import Single
from rxbp.acknowledgement.stopack import StopAck, stop_ack
from rxbp.observablesubjects.observablesubjectbase import ObservableSubjectBase
from rxbp.observer import Observer
from rxbp.observerinfo import ObserverInfo
from rxbp.typing import ElementType


@dataclass
class PublishObservableSubject(ObservableSubjectBase):
    class State(ABC):
        @abstractmethod
        def get_measured_state(self):
            ...

    @dataclass
    class NormalState(State):
        def get_measured_state(self):
            return self

    class CompletedState(State):
        def __init__(self):
            self.prev_state: PublishObservableSubject.State = None

        def get_measured_state(self):
            previous_state = self.prev_state.get_measured_state()

            if isinstance(previous_state, PublishObservableSubject.ExceptionState):
                return self.prev_state
            else:
                return self

    class ExceptionState(State):
        def __init__(self, exc):
            self.exc = exc

            self.prev_state = None

        def get_measured_state(self):
            return self

    def __post_init__(self):
        self.state = PublishObservableSubject.NormalState()
        self.subscriptions: List['PublishObservableSubject.InnerSubscription'] = []

        self.lock = threading.RLock()

    @dataclass
    class InnerSubscription:
        next_observer: Observer

    def observe(
            self,
            observer_info: ObserverInfo,
    ) -> rx.typing.Disposable:

        inner_subscription = self.InnerSubscription(
            next_observer=observer_info.observer,
        )

        with self.lock:
            prev_state = self.state
            self.subscriptions.append(inner_subscription)

        meas_state = prev_state.get_measured_state()

        if isinstance(meas_state, self.ExceptionState):
            observer_info.observer.on_error(meas_state.exc)

            try:
                self.subscriptions.remove(inner_subscription)
            except ValueError:
                pass

            return Disposable()

        elif isinstance(meas_state, self.CompletedState):
            observer_info.observer.on_completed()

            try:
                self.subscriptions.remove(inner_subscription)
            except ValueError:
                pass

            return Disposable()

        else:
            def dispose_func():
                with self.lock:
                    try:
                        self.subscriptions.remove(inner_subscription)
                    except ValueError:
                        pass

            return Disposable(dispose_func)

    def on_next(self, elem: ElementType):

        # received elements need to be materialized before being multi-casted
        if isinstance(elem, list):
            materialized_values = elem
        else:
            try:
                materialized_values = list(elem)
            except Exception as exc:
                self.on_error(exc)
                return stop_ack

        with self.lock:
            subscriptions = self.subscriptions.copy()

        if len(subscriptions) == 0:
            return continue_ack

        def gen_acks():
            for subscription in subscriptions:

                # synchronize on_next call to conform with Observer convention
                with self.lock:
                    ack = subscription.next_observer.on_next(materialized_values)

                if isinstance(ack, StopAck):
                    with self.lock:
                        try:
                            self.subscriptions.remove(subscription)
                        except ValueError:
                            pass

                        n_subscriptions = len(self.subscriptions)

                    if n_subscriptions == 0:
                        return stop_ack

                elif isinstance(ack, AckSubject):
                    @dataclass
                    class SubscriptionSingle(Single):
                        subscriptions: List['PublishObservableSubject.InnerSubscription']
                        subscription: 'PublishObservableSubject.InnerSubscription'
                        out_ack: AckSubject
                        lock: threading.RLock

                        def on_next(self, elem):
                            if isinstance(ack, StopAck):
                                with self.lock:
                                    try:
                                        self.subscriptions.remove(subscription)
                                    except ValueError:
                                        pass

                                    n_subscriptions = len(self.subscriptions)

                                if n_subscriptions == 0:
                                    self.out_ack.on_next(stop_ack)
                                    return

                            else:
                                self.out_ack.on_next(elem)

                    out_ack = AckSubject()
                    ack.subscribe(SubscriptionSingle(
                        subscriptions=self.subscriptions,
                        subscription=subscription,
                        out_ack=out_ack,
                        lock=self.lock,
                    ))
                    yield out_ack

                else:
                    yield ack

        acks = list(gen_acks())

        async_ack = [ack for ack in acks if not isinstance(ack, ContinueAck)]

        if len(async_ack) == 0:
            return continue_ack

        elif len(async_ack) == 1:
            return async_ack[0]

        else:
            return reduce_ack(
                sources=async_ack,
                func=lambda acc, v: v if isinstance(v, StopAck) else acc,
                initial=continue_ack,
            )

    def on_error(self, exc):
        state = self.ExceptionState(exc=exc)

        with self.lock:
            prev_state = self.state
            self.state = state

            # add item to buffer
            subscriptions = self.subscriptions.copy()
            self.subscriptions.clear()

        if isinstance(prev_state, self.NormalState):
            for subscription in subscriptions:
                subscription.next_observer.on_error(exc)

    def on_completed(self):
        state = self.CompletedState()

        with self.lock:
            prev_state = self.state
            self.state = state
            self.state.prev_state = prev_state

            # add item to buffer
            subscriptions = self.subscriptions.copy()
            self.subscriptions.clear()

        if isinstance(prev_state, self.NormalState):
            for subscription in subscriptions:
                subscription.next_observer.on_completed()
