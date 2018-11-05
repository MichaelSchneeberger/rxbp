import math

from rx import config
from rx.core import Disposable
from rx.core.notification import OnNext, OnCompleted, OnError, Notification
from rx.disposables import BooleanDisposable

from rxbackpressure.ack import Continue, Stop, Ack
from rxbackpressure.observable import Observable
from rxbackpressure.observer import Observer
from rxbackpressure.scheduler import SchedulerBase, ExecutionModel


class CachedServeFirstSubject(Observable, Observer):

    def __init__(self, name=None, scheduler=None):
        super().__init__()

        self.name = name
        self.scheduler = scheduler
        self.is_disposed = False
        self.is_stopped = False

        # track current index of each inner subscription
        self.current_index = {}

        # a inner subscription is inactive if all elements in the buffer are sent
        self.inactive_subsriptions = []

        self.buffer = self.DequeuableBuffer()
        self.exception = None
        self.ack = None

        self.lock = config["concurrency"].RLock()

    class DequeuableBuffer:
        def __init__(self):
            self.first_idx = 0
            self.queue = []

            self.lock = config["concurrency"].RLock()

        @property
        def last_idx(self):
            return self.first_idx + len(self.queue)

        def __len__(self):
            return len(self.queue)

        def has_element_at(self, idx):
            return idx <= self.last_idx

        def append(self, value):
            self.queue.append(value)

        def get(self, idx):
            if idx < self.first_idx:
                raise NameError('index {} is smaller than first index {}'.format(idx, self.first_idx))
            elif idx - self.first_idx >= len(self.queue):
                raise NameError(
                    'index {} is bigger or equal than length of queue {}'.format(idx - self.first_idx, len(self.queue)))
            return self.queue[idx - self.first_idx]

        def dequeue(self, idx):
            # empty buffer up until some index
            with self.lock:
                while self.first_idx <= idx and len(self.queue) > 0:
                    self.first_idx += 1
                    self.queue.pop(0)

    def _add_to_buffer(self, n: Notification) -> None:
        with self.lock:
            self.buffer.append(n)
            inactive_subscriptions = self.inactive_subsriptions
            self.inactive_subsriptions = []

        for inner_subscription in inactive_subscriptions:
            # notify only inactive subscriptions
            inner_subscription.notify()

    def unsafe_subscribe(self, observer, scheduler, subscribe_scheduler):
        # self.scheduler = self.scheduler or scheduler
        source = self
        em = scheduler.get_execution_model()

        class InnerSubscription:
            def __init__(self):
                self.last_ack = Continue()

            def notify(self):
                def _(v):
                    disposable = BooleanDisposable()

                    if isinstance(v, Continue):
                        self.fast_loop(source.current_index[self], 0, disposable)
                    else:
                        raise NotImplementedError
                # last acknowledgment might not be completed yet
                if isinstance(self.last_ack, Continue):
                    _(self.last_ack)
                elif isinstance(self.last_ack, Stop):
                    raise NotImplementedError
                else:
                    self.last_ack.observe_on(scheduler).subscribe(_)
                    # self.last_ack.observe_on(scheduler).subscribe(_)

            def fast_loop(self, current_idx: int, sync_index: int, disposable: BooleanDisposable):
                while True:
                    # buffer has an element at current_idx
                    notification = source.buffer.get(current_idx)
                    current_idx += 1

                    is_last = False
                    with source.lock:
                        # is this subscription last?
                        source.current_index[self] = current_idx
                        if min(source.current_index.values()) == current_idx:
                            # dequeing is required
                            is_last = True

                    try:
                        if is_last:
                            source.buffer.dequeue(current_idx - 1)

                        if isinstance(notification, OnCompleted):
                            observer.on_completed()
                            break
                        elif isinstance(notification, OnError):
                            observer.on_error(notification.exception)
                            break
                        else:
                            ack = observer.on_next(notification.value)

                        has_next = False
                        with source.lock:
                            # does it has element in the buffer?
                            if current_idx < source.buffer.last_idx:
                                has_next = True
                            else:
                                source.inactive_subsriptions.append(self)
                                self.last_ack = ack
                                source_ack = source.ack

                        if not has_next:
                            ack.subscribe(source_ack)
                            break
                        else:
                            if isinstance(ack, Continue):
                                next_index = em.next_frame_index(sync_index)
                            elif isinstance(ack, Stop):
                                next_index = -1
                            else:
                                next_index = 0

                            if next_index > 0:
                                sync_index = next_index
                            elif next_index == 0 and not disposable.is_disposed:
                                def on_next(next):
                                    if isinstance(next, Continue):
                                        try:
                                            self.fast_loop(current_idx, sync_index=0, disposable=disposable)
                                        except Exception as e:
                                            raise NotImplementedError
                                    else:
                                        raise NotImplementedError

                                def on_error(err):
                                    raise NotImplementedError

                                ack.observe_on(scheduler).subscribe(on_next=on_next, on_error=on_error)
                                break
                            else:
                                raise NotImplementedError
                    except:
                        raise Exception('fatal error')

        inner_subscription = InnerSubscription()

        with self.lock:
            if not self.is_stopped:
                # get current buffer index
                current_idx = self.buffer.first_idx
                self.current_index[inner_subscription] = current_idx
                # todo: is that ok?
                self.inactive_subsriptions.append(inner_subscription)
                return Disposable.empty()

            if self.exception:
                observer.on_error(self.exception)
                return Disposable.empty()

        observer.on_completed()
        return Disposable.empty()

    def on_next(self, value):
        self.ack = Ack()
        self._add_to_buffer(OnNext(value))
        return self.ack

    def on_completed(self):
        self._add_to_buffer(OnCompleted())

    def on_error(self, exception):
        self._add_to_buffer(OnError(exception))

    def dispose(self):
        """Unsubscribe all observers and release resources."""

        with self.lock:
            self.is_disposed = True
            self.current_index = None