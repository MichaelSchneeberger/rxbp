from rx import Observable
from rx.concurrency import immediate_scheduler
from rx.internal import extensionmethod
from rx.subjects import Subject

from rx_backpressure.core.anonymous_backpressure_observable import \
    AnonymousBackpressureObservable
from rx_backpressure.core.backpressure_base import BackpressureBase
from rx_backpressure.internal.blocking_future import BlockingFuture

Subject()

@extensionmethod(Observable)
def to_backpressure(self):
    class Buffer:
        def __init__(self):
            self.first_idx = 0
            self.queue = []

        @property
        def last_idx(self):
            return self.first_idx + len(self.queue) - 1

        def has_element_at(self, idx):
            return idx <= self.last_idx

        def append(self, value):
            self.queue.append(value)

        def get(self, idx):
            if idx < self.first_idx:
                raise NameError('index is smaller than first index')
            elif idx - self.first_idx >= len(self.queue):
                raise NameError('index is bigger than length of queue')
            return self.queue[idx - self.first_idx]

        def dequeue(self, idx):
            if self.first_idx <= idx:
                print('dequeue %s' % self.queue[0])
                self.first_idx += 1
                self.queue.pop(0)

                self.dequeue(idx)

    class ProxyBackpressure:
        def __init__(self, observer):
            self.observer = observer

        def request(self, number_of_items) -> BlockingFuture:
            future = BlockingFuture()
            self.requests.append((future, number_of_items))
            self._request_source()
            return future

    class BufferingBackpressure(BackpressureBase):
        def __init__(self, scheduler=None):
            self.scheduler = scheduler or immediate_scheduler
            self.buffer = []
            self.requests = []
            self.observer = None
            self.is_running = False

        def add_to_buffer(self, v):
            self.buffer.append(v)
            self._request_source()

        def request(self, number_of_items) -> BlockingFuture:
            print('1 request received, num = %s' %number_of_items)
            future = BlockingFuture()
            self.requests.append((future, number_of_items))
            self._request_source()
            return future

        def _request_source(self):
            if not self.is_running and len(self.requests) and self.observer:
                self.is_running = True

                def scheduled_action(a, s):
                    future, number_of_items = self.requests[0]
                    if number_of_items <= len(self.buffer):
                        for _ in range(number_of_items):
                            v = self.buffer.pop(0)
                            self.observer.on_next(v)
                            future.set(number_of_items)

                        self.is_running = False
                        self._request_source()
                    else:
                        self.is_running = False

                self.scheduler.schedule(scheduled_action)

    backpressure = BufferingBackpressure()

    def subscribe(observer):
        # with self.lock:
        #     self.check_disposed()
        #     if not self.is_stopped:
        #         self.observers.append(observer)
        #         return InnerSubscription(self, observer)
        #
        #     if self.exception:
        #         observer.on_error(self.exception)
        #         return Disposable.empty()
        #
        #     observer.on_completed()
        #     return Disposable.empty()



        backpressure.observer = observer
        def on_next(v):
            backpressure.add_to_buffer(v)
        disposable = self.subscribe(on_next, observer.on_error, observer.on_completed)
        return disposable

    return AnonymousBackpressureObservable(backpressure=backpressure, subscribe=subscribe)
