import threading

import rx

from typing import List


from rx.disposable import Disposable
from rx.core.notification import OnNext, OnCompleted, OnError
from rx.disposable import BooleanDisposable

from rxbp.ack import Continue, Stop, Ack, stop_ack
from rxbp.observable import Observable
from rxbp.observer import Observer
from rxbp.scheduler import ExecutionModel, Scheduler
from rxbp.subjects.subjectbase import SubjectBase


class CacheServeFirstSubject(SubjectBase):

    def __init__(self, scheduler: Scheduler, name=None):
        super().__init__()

        self.name = name
        self.scheduler = scheduler
        self.is_disposed = False
        self.is_stopped = False

        # track current index of each inner subscription
        self.current_index = {}

        # a inner subscription is inactive if all elements in the buffer are sent
        self.inactive_subsriptions: List[CacheServeFirstSubject.InnerSubscription] = []

        self.buffer = self.DequeuableBuffer()

        self.exception = None
        self.current_ack = None

        self.is_done = False

        self.lock = threading.RLock()

    class DequeuableBuffer:
        def __init__(self):
            self.first_idx = 0
            self.queue = []

        @property
        def last_idx(self):
            return self.first_idx + len(self.queue)

        def __len__(self):
            return len(self.queue)

        def has_element_at(self, idx):
            return idx < self.last_idx

        def append(self, value):
            self.queue.append(value)

        def get(self, idx):
            if idx < self.first_idx:
                raise Exception('index {} is smaller than first index {}'.format(idx, self.first_idx))
            elif idx - self.first_idx >= len(self.queue):
                raise Exception(
                    'index {} is bigger or equal than length of queue {}'.format(idx - self.first_idx, len(self.queue)))
            return self.queue[idx - self.first_idx]

        def dequeue(self, idx):
            # empty buffer up until some index
            # with self.lock:
            while self.first_idx <= idx and len(self.queue) > 0:
                self.first_idx += 1
                self.queue.pop(0)

    class InnerSubscription:
        def __init__(self, source: 'CacheServeFirstSubject', observer: Observer,
                     scheduler: Scheduler, em: ExecutionModel):
            self.source = source
            self.observer = observer
            self.scheduler = scheduler
            self.em = em

        def notify_on_next(self, value) -> Ack:
            # inner subscription gets only notified if all items from buffer are sent and ack received

            with self.source.lock:
                # increase current index
                self.source.current_index[self] += 1

            current_index = self.source.current_index[self]

            ack = self.observer.on_next(value)

            if isinstance(ack, Continue):
                self.source.inactive_subsriptions.append(self)
                return ack
            elif isinstance(ack, Stop):
                # with self.source.lock:
                #     del self.source.current_index[self]
                #     if len(self.source.current_ack) == 0:
                #         self.source.is_done = True
                self.signal_stop()
                return ack
            else:
                inner_ack = Ack()

                def _(v):
                    if isinstance(v, Continue):
                        with self.source.lock:
                            if current_index + 1 < self.source.buffer.last_idx:
                                has_elem = True
                            else:
                                # no new item has been added since call to 'notify_on_next'
                                has_elem = False
                                self.source.inactive_subsriptions.append(self)

                        if has_elem:
                            disposable = BooleanDisposable()
                            self.fast_loop(current_index, 0, disposable)
                    elif isinstance(v, Stop):
                        self.signal_stop()
                    else:
                        raise Exception('no recognized acknowledgment {}'.format(v))

                    inner_ack.on_next(v)
                    inner_ack.on_completed()

                ack.pipe(rx.operators.observe_on(scheduler=self.scheduler)).subscribe(_)
                return inner_ack

        def notify_on_completed(self):
            self.observer.on_completed()

        def notify_on_error(self, exc):
            self.observer.on_error(exc)

        def signal_stop(self):
            with self.source.lock:
                del self.source.current_index[self]

                # todo: bug here
                if len(self.source.current_index) == 0:
                    self.source.is_done = True

        def fast_loop(self, current_idx: int, sync_index: int, disposable: BooleanDisposable):
            while True:
                current_idx += 1

                # buffer has an element at current_idx
                notification = self.source.buffer.get(current_idx)

                is_last = False
                with self.source.lock:
                    # is this subscription last?
                    self.source.current_index[self] = current_idx
                    if min(self.source.current_index.values()) == current_idx:
                        # dequeing is required
                        self.source.buffer.dequeue(current_idx - 1)

                try:
                    if isinstance(notification, OnCompleted):
                        self.observer.on_completed()
                        break
                    elif isinstance(notification, OnError):
                        self.observer.on_error(notification.exception)
                        break
                    else:
                        ack = self.observer.on_next(notification.value)

                    has_next = False
                    with self.source.lock:
                        # does it has element in the buffer?
                        if current_idx + 1 < self.source.buffer.last_idx:
                            has_next = True
                        else:
                            if isinstance(ack, Continue):
                                self.source.inactive_subsriptions.append(self)
                                self.source.current_ack.on_next(ack)
                                self.source.current_ack.on_completed()
                            elif isinstance(ack, Stop):
                                self.signal_stop()
                                break
                            else:
                                def _(v):
                                    if isinstance(v, Continue):
                                        with self.source.lock:
                                            if current_idx + 1 < self.source.buffer.last_idx:
                                                has_elem = True
                                            else:
                                                has_elem = False
                                                self.source.inactive_subsriptions.append(self)
                                                self.source.current_ack.on_next(v)
                                                self.source.current_ack.on_completed()

                                        if has_elem:
                                            disposable = BooleanDisposable()
                                            self.fast_loop(current_idx, 0, disposable)
                                    elif isinstance(v, Stop):
                                        self.signal_stop()
                                    else:
                                        raise Exception('no recognized acknowledgment {}'.format(v))

                                ack.pipe(rx.operators.observe_on(self.scheduler)).subscribe(_)

                    if not has_next:
                        break
                    else:
                        if isinstance(ack, Continue):
                            next_index = self.em.next_frame_index(sync_index)
                        elif isinstance(ack, Stop):
                            next_index = -1
                        else:
                            next_index = 0

                        if next_index > 0:
                            sync_index = next_index
                        elif next_index == 0 and not disposable.is_disposed:
                            def on_next(v):
                                if isinstance(v, Continue):
                                    self.fast_loop(current_idx, sync_index=0, disposable=disposable)
                                elif isinstance(v, Stop):
                                    self.signal_stop()
                                else:
                                    raise Exception('no recognized acknowledgment {}'.format(v))

                            def on_error(err):
                                self.signal_stop()
                                self.observer.on_error(err)

                            ack.pipe(rx.operators.observe_on(self.scheduler)).subscribe(on_next=on_next, on_error=on_error)
                            break
                        else:
                            self.signal_stop()
                            break
                except:
                    raise Exception('fatal error')

    def observe(self, observer: Observer):
        em = self.scheduler.get_execution_model()
        inner_subscription = self.InnerSubscription(source=self, observer=observer, scheduler=self.scheduler, em=em)

        with self.lock:
            if not self.is_stopped:
                # get current buffer index
                current_idx = self.buffer.last_idx - 1
                self.current_index[inner_subscription] = current_idx
                self.inactive_subsriptions.append(inner_subscription)
                return Disposable()

            exception = self.exception

        if exception:
            observer.on_error(exception)
            return Disposable()

        observer.on_completed()
        return Disposable()

    def on_next(self, value):

        current_ack = Ack()

        with self.lock:
            # concurrent situation with acknowledgment in inner subscription or new subscriptions

            # empty inactive subscriptions; they are added to the list once they reach the top of the buffer again
            inactive_subsriptions = self.inactive_subsriptions
            self.inactive_subsriptions = []

            # add item to buffer
            self.buffer.append(OnNext(value))

            # current ack is used by subscriptions that weren't inactive, but reached the top of the buffer
            self.current_ack = current_ack

            is_done = self.is_done

        if is_done:
            return stop_ack

        def gen_inner_ack():
            # send notification to inactive subscriptions
            for inner_subscription in inactive_subsriptions:
                inner_ack = inner_subscription.notify_on_next(value)
                yield inner_ack

        inner_ack_list = list(gen_inner_ack())

        continue_ack = [ack for ack in inner_ack_list if isinstance(ack, Continue)]

        if 0 < len(continue_ack):
            # return any Continue ack
            return continue_ack[0]
        else:
            # return merged acknowledgments from inner subscriptions

            ack_list = [current_ack] + inner_ack_list

            upper_ack = Ack()
            rx.merge(*ack_list).pipe(rx.operators.first()).subscribe(upper_ack)
            return upper_ack

    def on_completed(self):
        with self.lock:
            # concurrent situation with acknowledgment in inner subscription or new subscriptions

            # inner subscriptions that return Continue or Stop need to be added to inactive subscriptions again
            inactive_subsriptions = self.inactive_subsriptions
            self.inactive_subsriptions = []

            # add item to buffer
            self.buffer.append(OnCompleted())

        # send notification to inactive subscriptions
        for inner_subscription in inactive_subsriptions:
            inner_subscription.notify_on_completed()

    def on_error(self, exception):
        with self.lock:
            # concurrent situation with acknowledgment in inner subscription or new subscriptions

            # inner subscriptions that return Continue or Stop need to be added to inactive subscriptions again
            inactive_subsriptions = self.inactive_subsriptions
            self.inactive_subsriptions = []

            # add item to buffer
            self.buffer.append(OnError(exception))

        # send notification to inactive subscriptions
        for inner_subscription in inactive_subsriptions:
            inner_subscription.notify_on_error(exception)

    def dispose(self):
        """Unsubscribe all observers and release resources."""

        with self.lock:
            self.is_disposed = True
            self.current_index = None
