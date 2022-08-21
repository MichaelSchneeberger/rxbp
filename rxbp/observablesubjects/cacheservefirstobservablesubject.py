import threading
import types
from abc import abstractmethod, ABC
from dataclasses import dataclass
from typing import List, Dict, Optional, Any, Tuple

import rx
from rx.core.notification import OnNext, OnCompleted, OnError, Notification
from rx.disposable import Disposable, SingleAssignmentDisposable

from rxbp.acknowledgement.acksubject import AckSubject
from rxbp.acknowledgement.continueack import ContinueAck, continue_ack
from rxbp.acknowledgement.ack import Ack
from rxbp.acknowledgement.operators.observeon import _observe_on
from rxbp.acknowledgement.single import Single
from rxbp.acknowledgement.stopack import StopAck, stop_ack
from rxbp.mixins.executionmodelmixin import ExecutionModelMixin
from rxbp.observablesubjects.observablesubjectbase import ObservableSubjectBase
from rxbp.observer import Observer
from rxbp.observerinfo import ObserverInfo
from rxbp.scheduler import Scheduler
from rxbp.typing import ElementType


@dataclass
class CacheServeFirstObservableSubject(ObservableSubjectBase):
    """ A observable Subject that does not back-pressure on a `on_next` call
    and buffers the last elements according to the slowest subscriber.
    """
    scheduler: Scheduler

    def __post_init__(self):
        self.shared_state = self.SharedState()

    class SharedState:
        """
        The shared state needs to be accessed via a lock
        """

        def __init__(self):

            self.lock = threading.RLock()

            self.state = CacheServeFirstObservableSubject.NormalState()

            # notification buffer
            # self.first_idx = -1
            self.queue: List[Notification] = []

            # contains inner subscriptions that are currently inactive, e.g. they sent
            # all elements in the buffer
            self.inactive_subscriptions = []

            # used for deque the buffer
            self.current_index: Dict['CacheServeFirstObservableSubject.InnerSubscription', int] = {}

            # the inner subscription reaching the end of the buffer requests a new element
            self.current_ack: Optional[AckSubject] = None

            self.is_disposed = False

        def add_inner_subscription(self, subscription):
            with self.lock:
                self.inactive_subscriptions.append(subscription)
                self.current_index[subscription] = len(self.queue)

        def dispose_subscription(self, subscription: 'CacheServeFirstObservableSubject.InnerSubscription'):
            with self.lock:
                del self.current_index[subscription]
                n_others = len(self.current_index)

                if subscription in self.inactive_subscriptions:
                    del self.inactive_subscriptions[subscription]

            if n_others == 0:
                self.current_ack.on_next(stop_ack)

        def dispose(self):
            self.is_disposed = True

            with self.lock:
                self.queue = None
                self.inactive_subscriptions = None
                self.current_index = None
                self.current_ack = None

                self.add_inner_subscription = types.MethodType(lambda _: None, self)
                self.on_next = types.MethodType(lambda _, __: ([], 0), self)
                self.on_completed = types.MethodType(lambda: [], self)
                self.on_error = types.MethodType(lambda _: [], self)
                self.get_element_for = types.MethodType(lambda _, __: (False, None), self)

        def get_element_for(
                self,
                subscription: 'CacheServeFirstObservableSubject.InnerSubscription',
        ) -> Tuple[bool, Any]:
            """
            returns the first element in the queue for a specific subscription, or 
            add subscription to inactive subscription
            """

            if self.is_disposed:
                return False, None

            with self.lock:
                if subscription not in self.current_index:
                    return False, None

                last_index = len(self.queue)
                index = self.current_index[subscription]

                has_elem = index < last_index

                # has items in buffer
                if has_elem:
                    notification = self.queue[index]

                    # update current index
                    self.current_index[subscription] += 1

                    # dequeue buffer if no subscription depend on first element
                    if 0 < min(self.current_index.values()):
                        self.queue.pop(0)

                        for key in self.current_index.keys():
                            self.current_index[key] -= 1

                # has no items in buffer
                else:
                    self.inactive_subscriptions.append(subscription)

                    if len(self.inactive_subscriptions) == 1:
                        self.current_ack.on_next(continue_ack)

                    notification = None

            return has_elem, notification

        def on_next(self, elem: ElementType, ack: AckSubject) -> Tuple[List, int]:
            with self.lock:
                self.queue.append(OnNext(elem))
                self.current_ack = ack

                inactive_subscriptions = self.inactive_subscriptions
                self.inactive_subscriptions = []

            return inactive_subscriptions

        def on_completed(self) -> List:
            state = CacheServeFirstObservableSubject.CompletedState()

            with self.lock:
                state.prev_state = self.state
                self.state = state

                self.queue.append(OnCompleted())

                inactive_subscriptions = self.inactive_subscriptions
                self.inactive_subscriptions = []

            return inactive_subscriptions, state.prev_state

        def on_error(self, exception: Exception) -> List:
            state = self.ExceptionState(exception)

            with self.lock:
                state.prev_state = self.state
                self.state = state

                self.queue.append(OnError(exception))

                inactive_subscriptions = self.inactive_subscriptions
                self.inactive_subscriptions = []

            return inactive_subscriptions, state.prev_state

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
            self.prev_state: CacheServeFirstObservableSubject.State = None

        def get_measured_state(self):
            previous_state = self.prev_state.get_measured_state()

            if isinstance(previous_state, CacheServeFirstObservableSubject.ExceptionState):
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
                shared_state: 'CacheServeFirstObservableSubject.SharedState',
                observer: Observer,
                scheduler: Scheduler,
                em: ExecutionModelMixin,
        ):
            self.shared_state = shared_state
            self.observer = observer
            self.scheduler = scheduler
            self.em = em

        @dataclass
        class AsyncAckSingle(Single):
            inner_subscription: 'CacheServeFirstObservableSubject.InnerSubscription'
            shared_state: 'CacheServeFirstObservableSubject.SharedState'

            def on_next(self, ack: Ack):
                
                if isinstance(ack, ContinueAck):
                    has_elem, notification = self.shared_state.get_element_for(
                        self.inner_subscription,
                    )

                    if has_elem:

                        # start fast_loop
                        self.inner_subscription.fast_loop(
                            notification, 
                            0,
                        )

                elif isinstance(ack, StopAck):
                    self.shared_state.dispose_subscription(self.inner_subscription)

                else:
                    raise Exception(f'acknowledgment {ack} not recognized')

            def on_error(self, exc: Exception):
                raise NotImplementedError

        def notify_on_next(
            self, 
            values: List,
        ) -> Optional[Ack]:
            """
            The inner subscription gets only notified if all items from buffer are sent, and
            last ack received. We say the subscription is inactive.
            """

            ack = self.observer.on_next(values)

            if isinstance(ack, ContinueAck):
                # append this subscription to inactive subscription list right away
                # not a concurrent since the next item is not send before all inner subscribers
                #   got notified
                self.shared_state.inactive_subscriptions.append(self)
                return ack

            elif isinstance(ack, StopAck):
                self.shared_state.dispose_subscription(self)
                return ack

            else:
                # keeps sending new elements from buffer once acknowledgment is received
                _observe_on(source=ack, scheduler=self.scheduler).subscribe(
                    self.AsyncAckSingle(
                        inner_subscription=self,
                        shared_state=self.shared_state,
                    )
                )

        def notify_on_completed(self):
            self.observer.on_completed()

        def notify_on_error(self, exc):
            self.observer.on_error(exc)

        def fast_loop(
                self, 
                notification: Notification, 
                sync_index: int,                    # used for execution model
            ):

            # a while loop instead of recursive function calls is faster and avoids a stack overflow error
            while True:

                # subscription is disposed
                if self not in self.shared_state.current_index:
                    break

                if isinstance(notification, OnCompleted):
                    self.observer.on_completed()
                    break

                else:
                    # for mypy to type check correctly
                    assert isinstance(notification.value, list)

                    ack = self.observer.on_next(notification.value)

                # synchronous or asynchronous acknowledgment
                if isinstance(ack, ContinueAck):
                    has_elem, notification = self.shared_state.get_element_for(self)

                    if has_elem:

                        # use frame index to either continue looping over elements or
                        # schedule to send element with a scheduler
                        next_index = self.em.next_frame_index(sync_index)

                        if next_index == 0:
                            self.fast_loop(
                                notification, 
                                sync_index=0,
                            )

                            break

                    else:
                        break

                elif isinstance(ack, StopAck):
                    self.shared_state.dispose_subscription(self)
                    break

                else:
                    # keeps sending new elements from buffer once acknowledgment is received
                    _observe_on(source=ack, scheduler=self.scheduler).subscribe(
                        self.AsyncAckSingle(
                            inner_subscription=self,
                            shared_state=self.shared_state,
                        )
                    )

                    break

    def observe(self, observer_info: ObserverInfo) -> rx.typing.Disposable:
        """ Create a new inner subscription and append it to the inactive subscriptions list (since there
        are no elements to be send yet)
        """

        em = self.scheduler.get_execution_model()
        inner_subscription = self.InnerSubscription(
            shared_state=self.shared_state,
            observer=observer_info.observer,
            scheduler=self.scheduler,
            em=em,
        )

        self.shared_state.add_inner_subscription(inner_subscription)

        # due to concurrency, on_completed or on_error might be called twice, which is ok
        meas_state = self.shared_state.state.get_measured_state()

        if isinstance(meas_state, self.ExceptionState):
            observer_info.observer.on_error(meas_state.exc)
            return Disposable()

        elif isinstance(meas_state, self.CompletedState):
            observer_info.observer.on_completed()
            return Disposable()

        else:
            def dispose_func():
                self.shared_state.dispose_subscription(inner_subscription)

            return Disposable(dispose_func)

    def on_next(self, elem: ElementType):

        if isinstance(elem, list):
            materialized_values = elem

        else:
            try:
                # received elements need to be materialized before being multi-casted
                materialized_values = list(elem)

            except Exception as exc:
                self.on_error(exc)
                return stop_ack

        current_ack = AckSubject()

        # add elements to the shared state
        inactive_subsriptions = self.shared_state.on_next(
            elem=materialized_values,
            ack=current_ack,
        )

        # send notification to inactive subscriptions
        def gen_inner_ack():
            for inner_subscription in inactive_subsriptions:

                # subscription is not disposed
                if inner_subscription in self.shared_state.current_index:
                    inner_ack = inner_subscription.notify_on_next(
                        materialized_values,
                    )
                    yield inner_ack

        inner_ack_list = list(gen_inner_ack())

        continue_acks = [ack for ack in inner_ack_list if isinstance(ack, ContinueAck)]

        # return any Continue
        if 0 < len(continue_acks):
            return continue_ack

        else:
            return current_ack

    def on_completed(self):
        # add complete item to buffer
        inactive_subsriptions, prev_state = self.shared_state.on_completed()

        # send notification to inactive subscriptions
        if isinstance(prev_state, self.NormalState):
            for inner_subscription in inactive_subsriptions:
                inner_subscription.notify_on_completed()

    def on_error(self, exc: Exception):
        # add error item to buffer
        inactive_subsriptions, prev_state = self.shared_state.on_completed()

        # send notification to inactive subscriptions
        if isinstance(prev_state, self.NormalState):
            for inner_subscription in inactive_subsriptions:
                inner_subscription.notify_on_error(exc)

    def dispose(self):
        # release resources
        self.shared_state.dispose()
