import sys
import threading
import types
from abc import abstractmethod, ABC
from dataclasses import dataclass
from typing import List, Dict, Optional, Any, Tuple

import rx
from rx.core.notification import OnNext, OnCompleted, OnError, Notification
from rx.disposable import Disposable, BooleanDisposable, CompositeDisposable, SingleAssignmentDisposable

from rxbp.ack.acksubject import AckSubject
from rxbp.ack.continueack import ContinueAck, continue_ack
from rxbp.ack.mixins.ackmixin import AckMixin
from rxbp.ack.operators.observeon import _observe_on
from rxbp.ack.single import Single
from rxbp.ack.stopack import StopAck, stop_ack
from rxbp.observablesubjects.osubjectbase import OSubjectBase
from rxbp.observer import Observer
from rxbp.observerinfo import ObserverInfo
from rxbp.scheduler import ExecutionModel, Scheduler
from rxbp.typing import ElementType


class CacheServeFirstOSubject(OSubjectBase):
    """ A observable Subject that does not back-pressure on a `on_next` call
    and buffers the last elements according to the slowest subscriber.
    """

    def __init__(self, scheduler: Scheduler, name=None):
        super().__init__()

        self.name = name
        self.scheduler = scheduler

        # mutable state
        # self.state = self.NormalState()
        self.shared_state = self.SharedState()

        self.lock = threading.RLock()

    class SharedState:
        """ Buffers all elements from the most recently received element to the earliest element
        that has not yet been sent to all subscribers
        """

        def __init__(self):

            self.state = CacheServeFirstOSubject.NormalState()

            # notification buffer
            self.first_idx = -1
            self.queue: List[Notification] = []

            # contains inner subscriptions that are currently inactive, e.g. they sent
            # all elements in the buffer
            self.inactive_subscriptions = []

            # used for deque the buffer
            self.current_index: Dict['CacheServeFirstOSubject.InnerSubscription', int] = {}

            self.subscriptions: List['CacheServeFirstOSubject.InnerSubscription'] = []

            # the inner subscription reaching the end of the buffer requests a new element
            self.current_ack: Optional[AckSubject] = None

            self.is_disposed = False

        def add_inner_subscription(self, subscription):
            self.inactive_subscriptions.append(subscription)
            self.subscriptions.append(subscription)

        def remove_subscription(self, subscription):
            if subscription in self.inactive_subscriptions:
                self.inactive_subscriptions.remove(subscription)
            self.subscriptions.remove(subscription)

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

        def get_element_for(
                self,
                subscription: 'CacheServeFirstOSubject.InnerSubscription',
                index: int,
                ack: AckMixin = None,
        ) -> Tuple[bool, Any]:

            if subscription.disposable.is_disposed:
                return False, None

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

            """
            Traceback (most recent call last):
              File "/home/mike/workspace/python/rxbackpressure/rxbp/observers/backpressurebufferedobserver.py", line 93, in signal_next
                ack = self.underlying.on_next(next)
              File "/home/mike/workspace/python/rxbackpressure/rxbp/observablesubjects/cacheservefirstosubject.py", line 353, in on_next
                dequeue_buffer = self.shared_state.should_dequeue(current_index)
              File "/home/mike/workspace/python/rxbackpressure/rxbp/observablesubjects/cacheservefirstosubject.py", line 109, in should_dequeue
                result = index <= min(self.current_index.values())
            ValueError: min() arg is an empty sequence
            """
            if not self.current_index:
                return False

            result = index <= min(self.current_index.values())
            return result

        def dequeue(self):
            self.first_idx += 1
            self.queue.pop(0)

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
            self.prev_state: CacheServeFirstOSubject.State = None

        def get_measured_state(self):
            previous_state = self.prev_state.get_measured_state()

            if isinstance(previous_state, CacheServeFirstOSubject.ExceptionState):
                return self.prev_state
            else:
                return self

    class ExceptionState(State):
        def __init__(self, exc):
            self.exc = exc

            self.prev_state = None

        def get_measured_state(self):
            return self

    class InnerSubscription:
        def __init__(
                self,
                shared_state: 'CacheServeFirstOSubject.SharedState',
                lock: threading.RLock,
                observer: Observer,
                scheduler: Scheduler,
                em: ExecutionModel,
                disposable: SingleAssignmentDisposable,
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
            ack_update: AckMixin = None

            def on_next(self, ack: AckMixin):
                # start fast_loop
                if isinstance(ack, ContinueAck):
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

                elif isinstance(ack, StopAck):
                    self.inner_subscription.signal_stop()

                else:
                    raise Exception(f'acknowledgment {ack} not recognized')

            def on_error(self, exc: Exception):
                raise NotImplementedError

        def notify_on_next(self, notification: Notification, current_index: int) -> Optional[AckMixin]:
            """ inner subscription gets only notified if all items from buffer are sent, and
            last ack received """

            # state is written without lock, because current_index is only used for dequeueing
            self.shared_state.current_index[self] = current_index

            ack = self.observer.on_next(notification)

            if isinstance(ack, ContinueAck):
                # append right away again to inactive subscription list
                self.shared_state.inactive_subscriptions.append(self)
                return ack

            elif isinstance(ack, StopAck):
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

                    else:
                        ack = self.observer.on_next(notification.value)

                    # synchronous or asynchronous acknowledgment
                    if isinstance(ack, ContinueAck):
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

                    elif isinstance(ack, StopAck):
                        self.signal_stop()
                        break

                    else:

                        meas_state = self.shared_state.state.get_measured_state()
                        if isinstance(meas_state, CacheServeFirstOSubject.ExceptionState):
                            break

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
        disposable = SingleAssignmentDisposable()
        inner_subscription = self.InnerSubscription(
            shared_state=self.shared_state,
            lock=self.lock,
            observer=observer,
            scheduler=self.scheduler,
            em=em,
            disposable=disposable,
        )

        def dispose_func():
            self.shared_state.remove_subscription(inner_subscription)

        disposable.disposable = Disposable(dispose_func)

        with self.lock:
            prev_state = self.shared_state.state
            self.shared_state.add_inner_subscription(inner_subscription)

        meas_state = prev_state.get_measured_state()

        if isinstance(meas_state, self.ExceptionState):
            observer.on_error(meas_state.exc)
            return Disposable()

        elif isinstance(meas_state, self.CompletedState):
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
            except Exception as exc:
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

        if all(isinstance(ack, StopAck) for ack in inner_ack_list):
            if len(inner_ack_list) == len(self.shared_state.subscriptions):
                return stop_ack

        dequeue_buffer = self.shared_state.should_dequeue(current_index)
        if dequeue_buffer:
            with self.lock:
                self.shared_state.dequeue()

        continue_ack = [ack for ack in inner_ack_list if isinstance(ack, ContinueAck)]

        # return any Continue or Stop ack
        if 0 < len(continue_ack):
            return continue_ack[0]

        else:
            return current_ack

    def on_completed(self):
        state = self.CompletedState()

        with self.lock:
            state.prev_state = self.shared_state.state
            self.state = state

            # add item to buffer
            inactive_subsriptions = self.shared_state.on_completed()

        # send notification to inactive subscriptions
        if isinstance(state.prev_state, self.NormalState):    # todo: necessary?
            for inner_subscription in inactive_subsriptions:
                inner_subscription.notify_on_completed()

    def on_error(self, exception: Exception):
        state = self.ExceptionState(exception)

        with self.lock:
            state.prev_state = self.shared_state.state
            self.state = state

            # # add item to buffer
            # inactive_subsriptions = self.shared_state.on_error(exception)

        subscriptions = self.shared_state.subscriptions

        # send notification to inactive subscriptions
        if isinstance(state.prev_state, self.NormalState):    # todo: necessary?
            for inner_subscription in subscriptions:
                inner_subscription.notify_on_error(exception)

    def dispose(self):
        """Unsubscribe all observers and release resources."""

        with self.lock:
            self.shared_state.dispose()
