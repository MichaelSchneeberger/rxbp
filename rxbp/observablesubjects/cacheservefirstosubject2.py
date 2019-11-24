import sys
import threading
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

            self.inactive_subscriptions = []
            self.current_ack: Optional[AckSubject] = None

        def get_element_for(self, subscription, index: int, ack: AckBase = None) -> Tuple[bool, Any]:
            if index < self.last_idx:
                return True, self.queue[index - self.first_idx]

            else:
                self.inactive_subscriptions.append(subscription)

                if ack is not None:
                    self.current_ack.on_next(ack)

                return False, None

        @property
        def last_idx(self):
            return self.first_idx + len(self.queue)

        # def __len__(self):
        #     return len(self.queue)

        # def has_element_at(self, idx):
        #     return idx < self.last_idx

        def on_next(self, elem: ElementType, ack: AckSubject):
            self.queue.append(OnNext(elem))
            self.current_ack = ack

        def on_completed(self):
            self.queue.append(OnCompleted())

        def on_error(self, exception: Exception):
            self.queue.append(OnError(exception))

        # def append(self, value):
        #     self.queue.append(value)
        #
        # def get(self, idx):
        #     # if idx < self.first_idx:
        #     #     raise Exception('index {} is smaller than first index {}'.format(idx, self.first_idx))
        #     # elif idx - self.first_idx >= len(self.queue):
        #     #     raise Exception(
        #     #         'index {} is bigger or equal than length of queue {}'.format(idx - self.first_idx, len(self.queue)))
        #     return self.queue[idx - self.first_idx]

        def dequeue(self, idx):
            # empty buffer up until some index
            while self.first_idx <= idx and len(self.queue) > 0:
                self.first_idx += 1
                self.queue.pop(0)

    class State(MeasuredState):
        pass

    # class InitState(State):
    #     def __init__(self):
    #         # current_idx = self.buffer.last_idx - 1
    #         # self.current_index[inner_subscription] = current_idx
    #         # self.inactive_subscriptions.append(inner_subscription)
    #
    #         # track current index of each inner subscription
    #         self.current_index = {}
    #
    #         # a inner subscription is inactive if all elements in the buffer are sent to the subscriber
    #         self.inactive_subscriptions: List[CacheServeFirstOSubject.InnerSubscription] = []

    # @dataclass
    # class InactiveState(State):
    #     pass

    @dataclass
    class NormalState(State):
        pass

        # buffer: 'CacheServeFirstOSubject.DequeuableBuffer'
        # current_indexes: Dict['CacheServeFirstOSubject.InnerSubscription', int]
        # inactive_subscriptions: List['CacheServeFirstOSubject.InnerSubscription']

        # def __init__(self, buffer: 'CacheServeFirstOSubject.DequeuableBuffer', current_index, inactive_subscriptions):
        #     self.buffer = buffer
        #
        #     # track current index of each inner subscription
        #     self.current_index = current_index
        #
        #     # a inner subscription is inactive if all elements in the buffer are sent to the subscriber
        #     self.inactive_subscriptions = inactive_subscriptions

    class CompletedState(State):
        pass

    class ExceptionState(State):
        def __init__(self, exc):
            self.exc = exc

    class RawState(RawStateNoArgs, ABC):
        pass

    class RawInitialState(RawState):
        def __init__(self):
            self.state = CacheServeFirstOSubject.NormalState(current_index={}, inactive_subscriptions=[])

    class RawOnSubscribeState(RawState):
        def __init__(
                self,
        ):

            self.prev_state: RawStateNoArgs = None
            self.meas_state = None

        def get_measured_state(self) -> MeasuredState:
            if self.meas_state is None:
                meas_prev_state = self.prev_state.get_measured_state()

                if isinstance(meas_prev_state, CacheServeFirstOSubject.ExceptionState) \
                        or isinstance(meas_prev_state, CacheServeFirstOSubject.CompletedState):
                    meas_state = meas_prev_state
                elif isinstance(meas_prev_state, CacheServeFirstOSubject.NormalState):
                    # inactive_subscriptions = meas_prev_state.inactive_subscriptions + [self.inner_subscription]
                    # current_indexes = {**meas_prev_state.current_indexes, **{self.inner_subscription, self.current_index}}

                    # meas_state = replace(meas_prev_state,
                    #                      inactive_subscriptions=inactive_subscriptions,
                    #                      current_indexes=current_indexes)
                    meas_state = meas_prev_state
                else:
                    raise NotImplementedError

                self.meas_state = meas_state

            else:
                meas_state = self.meas_state

            return meas_state

    class RawOnNextState(RawState):
        def __init__(
                self,
                elem: ElementType,
                current_index: int,
        ):
            self.elem = elem

            self.prev_state: RawStateNoArgs = None
            self.meas_state = None

        def get_measured_state(self) -> MeasuredState:
            if self.meas_state is None:
                meas_prev_state = self.prev_state.get_measured_state()

                if isinstance(meas_prev_state, CacheServeFirstOSubject.ExceptionState) \
                        or isinstance(meas_prev_state, CacheServeFirstOSubject.CompletedState):
                    meas_state = meas_prev_state
                elif isinstance(meas_prev_state, CacheServeFirstOSubject.NormalState):
                    # inactive_subscriptions = meas_prev_state.inactive_subscriptions
                    # current_indexes = {subscription:  for subscription in meas_prev_state.inactive_subscriptions}

                    meas_state = replace(meas_prev_state,
                                         inactive_subscriptions=[],
                                         current_indexes=current_indexes)
                else:
                    raise NotImplementedError

                self.meas_state = meas_state

            else:
                meas_state = self.meas_state

            return meas_state

    class RawOnCompletedState(RawState):
        pass

    class RawOnExceptionState(RawState):
        def __init__(self, exc):
            self.exc = exc

    def __init__(self, scheduler: Scheduler, name=None):
        super().__init__()

        self.name = name
        self.scheduler = scheduler
        self.is_disposed = False

        # non-concurrent
        # track current index of each inner subscription
        self.current_index: Dict['CacheServeFirstOSubject.InnerSubscription', int] = {}

        # concurrent access to
        # - inactive_subscription: Add subscriptions to the list, which need to be awaken when the
        #   next element is added to the buffer
        # - buffer: A running subscription reads elements from the buffer until there are no elements
        #   left

        # a inner subscription is inactive if all elements in the buffer are sent to the subscriber
        self.inactive_subscriptions: List[CacheServeFirstOSubject.InnerSubscription] = []

        # buffer elements according to the slowest inner subscriber
        self.buffer = self.SharedState()

        # exception received by the `on_error` method call
        self.exception = None

        self.current_ack = None

        self.state = self.RawInitialState()

        # used for concurrent access to fields of this class
        self.lock = threading.RLock()

    # @dataclass
    class InnerSubscription:
        # source: 'CacheServeFirstOSubject'
        # observer: Observer
        # scheduler: Scheduler
        # em: ExecutionModel

        def __init__(self, source: 'CacheServeFirstOSubject', observer: Observer,
                     scheduler: Scheduler, em: ExecutionModel):
            self.source = source
            self.observer = observer
            self.scheduler = scheduler
            self.em = em

            self.buffer = source.buffer

        def notify_on_next(self, notification) -> AckBase:
            """ inner subscription gets only notified if all items from buffer are sent, and
            last ack received """

            # non concurrent
            current_index = self.source.current_index[self] + 1
            self.source.current_index[self] = current_index

            ack = self.observer.on_next(notification)

            if isinstance(ack, Continue):
                # append right away again to inactive subscription list
                self.source.inactive_subscriptions.append(self)
                return ack

            elif isinstance(ack, Stop):
                self.signal_stop()
                return ack

            else:
                inner_ack_subject = AckSubject()

                def _(ack):

                    # start fast_loop
                    if isinstance(ack, Continue):
                        with self.source.lock:
                            has_elem, notification = self.buffer.get_element_for(self, current_index)

                        if has_elem:
                            disposable = BooleanDisposable()
                            self.fast_loop(current_index, notification, 0, disposable)

                    elif isinstance(ack, Stop):
                        self.signal_stop()

                    else:
                        raise Exception(f'acknowledgment {ack} not recognized')

                    inner_ack_subject.on_next(ack)

                class ResultSingle(Single):
                    def on_next(self, elem):
                        _(elem)

                    def on_error(self, exc: Exception):
                        raise NotImplementedError

                _observe_on(source=ack, scheduler=self.scheduler).subscribe(ResultSingle())
                return inner_ack_subject

        def notify_on_completed(self):
            self.observer.on_completed()

        def notify_on_error(self, exc):
            self.observer.on_error(exc)

        def signal_stop(self):
            with self.source.lock:
                del self.source.current_index[self]

        def fast_loop(self, current_index: int, notification: Any, sync_index: int, disposable: BooleanDisposable):
            while True:
                current_index += 1
                self.source.current_index[self] = current_index

                dequeue_buffer = current_index <= min(self.source.current_index.values())

                # this subscription is the slowest
                if dequeue_buffer:
                    with self.source.lock:
                        self.source.buffer.dequeue(current_index - 1)

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
                        with self.source.lock:
                            has_elem, notification = self.buffer.get_element_for(self, current_index, ack)

                        if has_elem:
                            next_index = self.em.next_frame_index(sync_index)

                            if next_index == 0 and not disposable.is_disposed:
                                self.fast_loop(current_index, notification, sync_index=0, disposable=disposable)
                                break
                        else:
                            self.source.current_ack.on_next(ack)
                            break
                    if isinstance(ack, Stop):
                        self.signal_stop()
                        break
                    else:
                        def _(ack_val):
                            if isinstance(ack_val, Continue):
                                with self.source.lock:
                                    has_elem, notification = self.buffer.get_element_for(self, current_index, ack)

                                if has_elem:
                                    disposable = BooleanDisposable()
                                    self.fast_loop(current_index, notification, 0, disposable)
                            elif isinstance(ack_val, Stop):
                                self.signal_stop()
                            else:
                                raise Exception('no recognized acknowledgment {}'.format(ack_val))

                        class ResultSingle(Single):
                            def on_next(self, elem):
                                _(elem)

                            def on_error(self, exc: Exception):
                                raise NotImplementedError

                        _observe_on(source=ack, scheduler=self.scheduler).subscribe(ResultSingle())
                except:
                    raise Exception('fatal error')

    def observe(self, observer_info: ObserverInfo):
        """ Create a new inner subscription and append it to the inactive subscriptions list (because there
        are no elements to be send yet)
        """

        observer = observer_info.observer
        em = self.scheduler.get_execution_model()
        inner_subscription = self.InnerSubscription(
            source=self,
            observer=observer,
            scheduler=self.scheduler,
            em=em,
        )

        self.inactive_subscriptions.append(inner_subscription)

        state = self.RawOnSubscribeState(
            # inner_subscription=inner_subscription,
            # current_index=self.buffer.last_idx,
        )

        with self.lock:
            prev_state = self.state
            self.state = state
            state.prev_state = prev_state

        if isinstance(state, self.ExceptionState):
            self.inactive_subscriptions.pop(0)
            observer.on_error(state.exc)
            return Disposable()
        elif isinstance(state, self.CompletedState):
            self.inactive_subscriptions.pop(0)
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
            # add item to buffer
            self.buffer.on_next(elem=materialized_values, ack=current_ack)

            # empty inactive subscriptions; they are added to the list once they reach the top of the buffer again
            # concurrent access to inactive_subscription with
            # - acknowledgment in inner subscription that adds itself to inactive_subscriptions
            # - new subscription that adds itself to the inactive_subscriptions
            inactive_subsriptions = self.inactive_subscriptions
            self.inactive_subscriptions = []

            # # current ack is used by subscriptions that are not currently inactive, but reach the top of the buffer
            # # before the next `on_next` method call
            # # value has to be synchronized to the last OnNext notification added to the buffer
            # self.current_ack = current_ack

        # send notification to inactive subscriptions
        def gen_inner_ack():
            for inner_subscription in inactive_subsriptions:
                inner_ack = inner_subscription.notify_on_next(materialized_values)
                yield inner_ack
        inner_ack_list = list(gen_inner_ack())

        continue_ack = [ack for ack in inner_ack_list if isinstance(ack, Continue)]

        if 0 < len(continue_ack):
            # return any Continue ack
            return continue_ack[0]
        else:
            # return merged acknowledgments from inner subscriptions

            async_ack = [ack for ack in inner_ack_list if not isinstance(ack, Continue) and not isinstance(ack, Stop)]

            ack_list = [current_ack] + async_ack

            upper_ack = AckSubject()

            for ack in ack_list:
                ack.subscribe(upper_ack)

            return upper_ack

    def on_completed(self):
        with self.lock:
            # concurrent situation with acknowledgment in inner subscription or new subscriptions

            # add item to buffer
            self.buffer.on_completed()

            # inner subscriptions that return Continue or Stop need to be added to inactive subscriptions again
            inactive_subsriptions = self.inactive_subscriptions
            self.inactive_subscriptions = []

        # send notification to inactive subscriptions
        for inner_subscription in inactive_subsriptions:
            inner_subscription.notify_on_completed()

    def on_error(self, exception: Exception):
        with self.lock:
            # add item to buffer
            self.buffer.on_error(exception)

            # concurrent situation with acknowledgment in inner subscription or new subscriptions
            # inner subscriptions that return Continue or Stop need to be added to inactive subscriptions again
            inactive_subsriptions = self.inactive_subscriptions
            self.inactive_subscriptions = []

            self.exception = exception

        # send notification to inactive subscriptions
        for inner_subscription in inactive_subsriptions:
            inner_subscription.notify_on_error(exception)

    def dispose(self):
        """Unsubscribe all observers and release resources."""

        with self.lock:
            self.is_disposed = True
            self.current_index = None
