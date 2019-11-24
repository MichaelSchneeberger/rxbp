import sys
import threading
import types
from abc import ABC
from dataclasses import dataclass, replace

from typing import List, Dict, Optional, Any, Tuple

from rx.disposable import Disposable
from rx.core.notification import OnNext, OnCompleted, OnError
from rx.disposable import BooleanDisposable
from rxbp.ack.ackimpl import Continue, Stop, stop_ack
from rxbp.ack.ackbase import AckBase
from rxbp.ack.acksubject import AckSubject
from rxbp.ack.observeon import _observe_on
from rxbp.ack.single import Single
from rxbp.observer import Observer

from rxbp.observerinfo import ObserverInfo
from rxbp.scheduler import ExecutionModel, Scheduler
from rxbp.observablesubjects.osubjectbase import OSubjectBase
from rxbp.states.measuredstates.measuredstate import MeasuredState
from rxbp.states.rawstates.rawstatenoargs import RawStateNoArgs
from rxbp.typing import ElementType


class CacheServeFirstOSubject(OSubjectBase):
    """ A observable Subject that does not back-pressure on a `on_next` call
    and buffers the last elements according to the slowest subscriber.
    """

    class SharedState:
        """ Buffers all elements from the most recently received element to the earliest element
        that has not yet been sent to all subscribers
        """

        def __init__(self):
            self.first_idx = -1
            self.queue = []

            self.is_disposed = False

            self.inactive_subscriptions = []
            self.current_index: Dict['CacheServeFirstOSubject.InnerSubscription', int] = {}
            self.current_ack: Optional[AckSubject] = None

        def add_inner_subscription(self, subscription):
            self.inactive_subscriptions.append(subscription)

        def dispose(self):
            self.is_disposed = True

            self.queue = None
            self.inactive_subscriptions = None
            self.current_index = None
            self.current_ack = None

            self.add_inner_subscription = types.MethodType(lambda _: None, self)
            self.on_next = types.MethodType(lambda _, __: ([], 0), self)
            self.on_completed = types.MethodType(lambda: [], self)
            self.on_error = types.MethodType(lambda _: [], self)
            self.get_element_for = types.MethodType(lambda _, __, ___: (False, None), self)
            self.should_dequeue = types.MethodType(lambda _: False, self)
            self.dequeue = types.MethodType(lambda: None, self)

        def get_element_for(self, subscription, index: int, ack: AckBase = None) -> Tuple[bool, Any]:
            last_index = self.first_idx + len(self.queue)

            if index < last_index:
                return True, self.queue[index - self.first_idx]

            else:
                self.inactive_subscriptions.append(subscription)

                if ack is not None:
                    self.current_ack.on_next(ack)

                return False, None

        def on_next(self, elem: ElementType, ack: AckSubject) -> Tuple[List, int]:
            self.queue.append(OnNext(elem))
            self.current_ack = ack

            inactive_subscriptions = self.inactive_subscriptions
            self.inactive_subscriptions = []

            last_index = self.first_idx + len(self.queue)

            return inactive_subscriptions, last_index

        def on_completed(self) -> List:
            self.queue.append(OnCompleted())

            inactive_subscriptions = self.inactive_subscriptions
            self.inactive_subscriptions = []

            return inactive_subscriptions

        def on_error(self, exception: Exception) -> List:
            self.queue.append(OnError(exception))

            inactive_subscriptions = self.inactive_subscriptions
            self.inactive_subscriptions = []

            return inactive_subscriptions

        def should_dequeue(self, index: int):
            return index <= min(self.current_index.values())

        def dequeue(self):
            self.first_idx += 1
            self.queue.pop(0)

    # class SharedState:
    #     pass
    #
    # class RawSharedState:
    #     pass
    #
    # @dataclass
    # class OnNext(RawSharedState):
    #     elem: ElementType
    #
    #     prev_state: None

    class State(MeasuredState):
        pass

    @dataclass
    class NormalState(State):
        pass

    class CompletedState(State):
        pass

    class ExceptionState(State):
        def __init__(self, exc):
            self.exc = exc

    # class RawState(RawStateNoArgs, ABC):
    #     pass
    #
    # class RawNormalState(RawState):
    #     pass
    #
    # class RawOnCompletedState(RawState):
    #     pass
    #
    # class RawOnExceptionState(RawState):
    #     def __init__(self, exc):
    #         self.exc = exc

    def __init__(self, scheduler: Scheduler, name=None):
        super().__init__()

        self.name = name
        self.scheduler = scheduler

        # mutable state
        self.state = self.NormalState()
        self.shared_state = self.SharedState()

        self.lock = threading.RLock()

    class InnerSubscription:
        def __init__(
                self,
                shared_state: 'CacheServeFirstOSubject.SharedState',
                lock: threading.RLock,
                observer: Observer,
                scheduler: Scheduler,
                em: ExecutionModel,
        ):
            self.shared_state = shared_state
            self.lock = lock
            self.observer = observer
            self.scheduler = scheduler
            self.em = em

        @dataclass
        class AsyncAckSingle(Single):
            current_index: int
            inner_subscription: 'CacheServeFirstOSubject.InnerSubscription'
            on_next_ack: AckSubject = None

            def on_next(self, ack: AckBase):
                # start fast_loop
                if isinstance(ack, Continue):
                    with self.inner_subscription.lock:
                        has_elem, notification = self.inner_subscription.shared_state.get_element_for(
                            self.inner_subscription,
                            self.current_index,
                        )

                    if has_elem:
                        disposable = BooleanDisposable()
                        self.inner_subscription.fast_loop(self.current_index, notification, 0, disposable)
                    else:
                        pass

                elif isinstance(ack, Stop):
                    self.inner_subscription.signal_stop()

                else:
                    raise Exception(f'acknowledgment {ack} not recognized')

                if self.on_next_ack is not None:
                    self.on_next_ack.on_next(ack)

            def on_error(self, exc: Exception):
                raise NotImplementedError

        def notify_on_next(self, notification, current_index: int) -> AckBase:
            """ inner subscription gets only notified if all items from buffer are sent, and
            last ack received """

            # non concurrent
            self.shared_state.current_index[self] = current_index

            ack = self.observer.on_next(notification)

            if isinstance(ack, Continue):
                # append right away again to inactive subscription list
                self.shared_state.inactive_subscriptions.append(self)
                return ack

            elif isinstance(ack, Stop):
                self.signal_stop()
                return ack

            else:
                on_next_ack = AckSubject()

                single = self.AsyncAckSingle(
                    inner_subscription=self,
                    current_index=current_index,
                    on_next_ack=on_next_ack,
                )

                _observe_on(source=ack, scheduler=self.scheduler).subscribe(single)
                return on_next_ack

        def notify_on_completed(self):
            self.observer.on_completed()

        def notify_on_error(self, exc):
            self.observer.on_error(exc)

        def signal_stop(self):
            with self.lock:
                del self.shared_state.current_index[self]

        def fast_loop(self, current_index: int, notification: Any, sync_index: int, disposable: BooleanDisposable):
            while True:
                current_index += 1
                self.shared_state.current_index[self] = current_index

                dequeue_buffer = self.shared_state.should_dequeue(current_index)
                if dequeue_buffer:
                    with self.lock:
                        self.shared_state.dequeue()

                try:
                    if isinstance(notification, OnCompleted):
                        self.observer.on_completed()
                        break

                    elif isinstance(notification, OnError):
                        self.observer.on_error(notification.exception)
                        break

                    else:
                        ack = self.observer.on_next(notification.value)

                    # synchronous or asynchronous acknowledgment
                    if isinstance(ack, Continue):
                        with self.lock:
                            has_elem, notification = self.shared_state.get_element_for(self, current_index, ack)

                        if has_elem:
                            next_index = self.em.next_frame_index(sync_index)

                            if next_index == 0 and not disposable.is_disposed:
                                self.fast_loop(current_index, notification, sync_index=0, disposable=disposable)
                                break

                            else:
                                pass

                        else:
                            self.shared_state.current_ack.on_next(ack)
                            break

                    if isinstance(ack, Stop):
                        self.signal_stop()
                        break

                    else:
                        single = self.AsyncAckSingle(
                            inner_subscription=self,
                            current_index=current_index,
                        )

                        _observe_on(source=ack, scheduler=self.scheduler).subscribe(single)
                        break
                except:
                    raise Exception('fatal error')

    def observe(self, observer_info: ObserverInfo):
        """ Create a new inner subscription and append it to the inactive subscriptions list (because there
        are no elements to be send yet)
        """

        observer = observer_info.observer
        em = self.scheduler.get_execution_model()
        inner_subscription = self.InnerSubscription(
            shared_state=self.shared_state,
            lock=self.lock,
            observer=observer,
            scheduler=self.scheduler,
            em=em,
        )

        with self.lock:
            prev_state = self.state

            self.shared_state.add_inner_subscription(inner_subscription)

        if isinstance(prev_state, self.ExceptionState):
            observer.on_error(prev_state.exc)
            return Disposable()

        elif isinstance(prev_state, self.CompletedState):
            observer.on_completed()
            return Disposable()

        else:
            pass

    def on_next(self, elem: ElementType):

        # received element need to be materialized when being multi-casted
        if isinstance(elem, list):
            materialized_values = elem
        else:
            try:
                materialized_values = list(elem)
            except:
                exc = sys.exc_info()
                self.on_error(exc)
                return stop_ack

        current_ack = AckSubject()

        with self.lock:
            inactive_subsriptions, current_index = self.shared_state.on_next(elem=materialized_values, ack=current_ack)

        # send notification to inactive subscriptions
        def gen_inner_ack():
            for inner_subscription in inactive_subsriptions:
                inner_ack = inner_subscription.notify_on_next(materialized_values, current_index)
                yield inner_ack
        inner_ack_list = list(gen_inner_ack())

        continue_ack = [ack for ack in inner_ack_list if isinstance(ack, Continue)]

        if 0 < len(continue_ack):
            # return any Continue ack
            return continue_ack[0]

        # return merged acknowledgments from inner subscriptions
        else:
            async_ack = [ack for ack in inner_ack_list if not isinstance(ack, Continue) and not isinstance(ack, Stop)]

            ack_list = [current_ack] + async_ack

            upper_ack = AckSubject()

            for ack in ack_list:
                ack.subscribe(upper_ack)

            return upper_ack

    def on_completed(self):
        state = self.CompletedState()

        with self.lock:
            prev_state = self.state
            self.state = state

            # add item to buffer
            inactive_subsriptions = self.shared_state.on_completed()

        # send notification to inactive subscriptions
        if isinstance(prev_state, self.NormalState):
            for inner_subscription in inactive_subsriptions:
                inner_subscription.notify_on_completed()

    def on_error(self, exception: Exception):
        state = self.ExceptionState(exception)

        with self.lock:
            prev_state = self.state
            self.state = state

            # add item to buffer
            inactive_subsriptions = self.shared_state.on_error(exception)

        # send notification to inactive subscriptions
        if isinstance(prev_state, self.NormalState):
            for inner_subscription in inactive_subsriptions:
                inner_subscription.notify_on_error(exception)

    def dispose(self):
        """Unsubscribe all observers and release resources."""

        with self.lock:
            self.shared_state.dispose()
