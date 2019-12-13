import sys
import threading
import types
from dataclasses import dataclass
from typing import List, Dict, Optional, Any, Tuple

import rx
from rx.core.notification import OnNext, OnCompleted, OnError, Notification
from rx.disposable import Disposable, BooleanDisposable
from rxbp.ack.ackbase import AckBase
from rxbp.ack.ackimpl import Continue, Stop, stop_ack, continue_ack
from rxbp.ack.acksubject import AckSubject
from rxbp.ack.observeon import _observe_on
from rxbp.ack.single import Single
from rxbp.observablesubjects.osubjectbase import OSubjectBase
from rxbp.observer import Observer
from rxbp.observerinfo import ObserverInfo
from rxbp.scheduler import ExecutionModel, Scheduler
from rxbp.states.measuredstates.measuredstate import MeasuredState
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

            # notification buffer
            self.first_idx = -1
            self.queue: List[Notification] = []

            # contains inner subscriptions that are currently inactive, e.g. they sent
            # all elements in the buffer
            self.inactive_subscriptions = []

            # used for deque the buffer
            self.current_index: Dict['CacheServeFirstOSubject.InnerSubscription', int] = {}

            # the inner subscription reaching the end of the buffer requests a new element
            self.current_ack: Optional[AckSubject] = None

        def add_inner_subscription(self, subscription):
            self.inactive_subscriptions.append(subscription)

        def dispose(self):

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
            result = index <= min(self.current_index.values())
            return result

        def dequeue(self):
            self.first_idx += 1
            self.queue.pop(0)

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
                disposable: BooleanDisposable,
        ):
            self.shared_state = shared_state
            self.lock = lock
            self.observer = observer
            self.scheduler = scheduler
            self.em = em
            self.disposable = disposable

        @dataclass
        class AsyncAckSingle(Single):
            current_index: int
            inner_subscription: 'CacheServeFirstOSubject.InnerSubscription'
            ack_update: AckBase = None

            def on_next(self, ack: AckBase):
                # start fast_loop
                if isinstance(ack, Continue):
                    with self.inner_subscription.lock:
                        has_elem, notification = self.inner_subscription.shared_state.get_element_for(
                            self.inner_subscription,
                            self.current_index,
                            self.ack_update
                        )

                    if has_elem:
                        self.inner_subscription.fast_loop(self.current_index, notification, 0)

                    else:
                        pass

                elif isinstance(ack, Stop):
                    self.inner_subscription.signal_stop()

                else:
                    raise Exception(f'acknowledgment {ack} not recognized')

            def on_error(self, exc: Exception):
                raise NotImplementedError

        def notify_on_next(self, notification: Notification, current_index: int) -> Optional[AckBase]:
            """ inner subscription gets only notified if all items from buffer are sent, and
            last ack received """

            # state is written without lock, because current_index is only used for dequeueing
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
                single = self.AsyncAckSingle(
                    inner_subscription=self,
                    current_index=current_index,
                    ack_update=continue_ack,
                )

                _observe_on(source=ack, scheduler=self.scheduler).subscribe(single)
                return None

        def notify_on_completed(self):
            self.observer.on_completed()

        def notify_on_error(self, exc):
            self.observer.on_error(exc)

        def signal_stop(self):
            with self.lock:
                del self.shared_state.current_index[self]

        def fast_loop(self, current_index: int, notification: Notification, sync_index: int):

            # a while loop instead of recursive function calls is faster and avoids a stack overflow error
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
                            has_elem, notification = self.shared_state.get_element_for(
                                self, current_index, ack)

                        if has_elem:

                            # use frame index to either continue looping over elements or
                            # schedule to send element with a scheduler
                            next_index = self.em.next_frame_index(sync_index)

                            if 0 < next_index:
                                continue

                            else:
                                self.fast_loop(current_index, notification, sync_index=0)
                                break

                        else:
                            break

                    elif isinstance(ack, Stop):
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
                    pass

                raise Exception('fatal error')

    def observe(self, observer_info: ObserverInfo) -> rx.typing.Disposable:
        """ Create a new inner subscription and append it to the inactive subscriptions list (because there
        are no elements to be send yet)
        """

        observer = observer_info.observer
        em = self.scheduler.get_execution_model()
        disposable = BooleanDisposable()
        inner_subscription = self.InnerSubscription(
            shared_state=self.shared_state,
            lock=self.lock,
            observer=observer,
            scheduler=self.scheduler,
            em=em,
            disposable=disposable,
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
            return disposable

    def on_next(self, elem: ElementType):

        # received elements need to be materialized before being multi-casted
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
            inactive_subsriptions, current_index = self.shared_state.on_next(
                elem=materialized_values, ack=current_ack)

        # send notification to inactive subscriptions
        def gen_inner_ack():
            for inner_subscription in inactive_subsriptions:
                inner_ack = inner_subscription.notify_on_next(
                    materialized_values, current_index)
                yield inner_ack
        inner_ack_list = list(gen_inner_ack())

        dequeue_buffer = self.shared_state.should_dequeue(current_index)
        if dequeue_buffer:
            with self.lock:
                self.shared_state.dequeue()

        continue_ack = [ack for ack in inner_ack_list if isinstance(ack, Continue)]

        # return any Continue or Stop ack
        if 0 < len(continue_ack):
            return continue_ack[0]

        else:
            return current_ack

    def on_completed(self):
        state = self.CompletedState()

        with self.lock:
            prev_state = self.state
            self.state = state

            # add item to buffer
            inactive_subsriptions = self.shared_state.on_completed()

        # send notification to inactive subscriptions
        if isinstance(prev_state, self.NormalState):    # todo: necessary?
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
        if isinstance(prev_state, self.NormalState):    # todo: necessary?
            for inner_subscription in inactive_subsriptions:
                inner_subscription.notify_on_error(exception)

    def dispose(self):
        """Unsubscribe all observers and release resources."""

        with self.lock:
            self.shared_state.dispose()
