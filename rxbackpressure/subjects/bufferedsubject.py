import math

from rx import config, Observer
from rx.concurrency import immediate_scheduler, current_thread_scheduler
from rx.core import Disposable, ObserverBase
from rx.core.notification import OnNext, OnCompleted
from rx.internal import DisposedException

from rxbackpressure.buffers.dequeuablebuffer import DequeuableBuffer
from rxbackpressure.core.backpressureobservable import BackpressureObservable
from rxbackpressure.internal.blockingfuture import BlockingFuture


class BufferedSubject(BackpressureObservable, Observer):
    class ProxyBufferingFirst(ObserverBase, BackpressureObservable):
        def __init__(self, buffer, last_idx, observer, update_source, scheduler=None):
            super().__init__()

            self.observer = observer
            self.buffer = buffer
            self.current_idx = last_idx
            self.requests = []
            self.is_stopped = False
            self._lock = config["concurrency"].RLock()
            self.scheduler = scheduler or current_thread_scheduler
            self.is_disposed = False
            self.update_source = update_source

        def check_disposed(self):
            if self.is_disposed:
                raise DisposedException()

        def request(self, number_of_items) -> BlockingFuture:
            if number_of_items is math.inf:
                # dispose Proxy
                self.dispose()

            # self.check_disposed()
            future = BlockingFuture()
            # print('request %s' % number_of_items)
            def action(a, s):
                # print('is stopped %s' % self.is_stopped)
                if not self.is_stopped:
                    with self._lock:
                        self.requests.append((future, number_of_items, 0))
                    self.update()

                    # inform source about update
                    self.update_source(self, self.current_idx)
                else:
                    future.set(0)
            self.scheduler.schedule(action)
            return future

        def _empty_requests(self):
            # print('empty requests')
            with self._lock:
                requests = self.requests
                self.requests = []

            def action(a, s):
                if requests:
                    for future, _, __ in requests:
                        future.set(0)
                        # print('ok')
            self.scheduler.schedule(action)

        def update(self) -> int:
            def take_requests():
                # print('current idx %s' % self.current_idx)
                # print('last idx %s' % self.buffer.last_idx)
                for future, number_of_items, counter in self.requests:
                    if self.current_idx <= self.buffer.last_idx:
                        def get_value_from_buffer():
                            for _ in range(d_number_of_items):
                                value = self.buffer.get(self.current_idx)
                                self.current_idx += 1
                                yield value
                        # there are new items in buffer
                        if self.current_idx + number_of_items - counter <= self.buffer.last_idx:
                            # request fully fullfilled
                            d_number_of_items = number_of_items - counter
                            yield None, list(get_value_from_buffer()), (future, number_of_items)
                        else:
                            # request not fully fullfilled
                            d_number_of_items = self.buffer.last_idx - self.current_idx
                            yield (future, number_of_items, counter + d_number_of_items), list(get_value_from_buffer()), None
                    else:
                        # there are no new items in buffer
                        yield (future, number_of_items, counter), None, None

            if self.observer:
                # take as many requests as possible from self.requests
                has_elements = False
                with self._lock:
                    if len(self.requests):
                        has_elements = True
                        request_list, buffer_value_list, future_tuple_list = zip(*take_requests())
                        self.requests = [request for request in request_list if request is not None]

                # send values at some later time
                if has_elements is True:
                    def action(a, s):
                        # set future from request
                        future_tuple_list_ = [e for e in future_tuple_list if e is not None]
                        for future, number_of_items in future_tuple_list_:
                            # print('future %s' % future)
                            future.set(number_of_items)
                            # print('delete request')

                        # send values taken from buffer
                        value_to_send = [e for value_list in buffer_value_list if value_list is not None for e in value_list]
                        self.send_values(value_to_send)

                    self.scheduler.schedule(action)

            # return current index in shared buffer
            return self.current_idx

        def send_values(self, value_list):
            # print('current idx %s' % self.current_idx)
            # print('send # of values = %s' % value_list)
            # def action(a, s):
            for value in value_list:
                # print(value)
                if isinstance(value, OnNext):
                    self.observer.on_next(value.value)
                else:
                    # print('send on completed')
                    self.is_stopped = True
                    self.observer.on_completed()
                    self._empty_requests()
            # self.scheduler.schedule(action)

        def _on_next_core(self, value):
            return NotImplemented

        def _on_error_core(self, error):
            if self.observer:
                self.observer.on_error(error)

        def _on_completed_core(self):
            if self.observer:
                self.observer.on_completed()

        def dispose(self):
            with self._lock:
                self.is_disposed = True
                self.is_stopped = True
                self.requests = None
                self.observer = None

    def __init__(self, scheduler=None, release_buffer=None):
        super().__init__()

        self.scheduler = scheduler or current_thread_scheduler
        self.is_disposed = False
        self.is_stopped = False
        self.proxies = {}
        self.buffer = DequeuableBuffer()        #BufferedSubject.Buffer()
        self.exception = None
        self.release_buffer = release_buffer

        if release_buffer:
            release_buffer.subscribe(lambda v: self._request_source())

        self.lock = config["concurrency"].RLock()

    def _add_to_buffer(self, v):
        # print('add to buffer %s' % v)
        if not self.is_disposed:
            self.buffer.append(v)
            self._request_source()
        else:
            print('disposed')

    def _request_source(self):
        def scheduled_action(a, s):
            if self.proxies and len(self.proxies.items()):
                with self.lock:
                    for proxy in self.proxies.keys():
                        self.proxies[proxy] = proxy.update()
                self._dequeue_buffer()

        self.scheduler.schedule(scheduled_action)

    def _dequeue_buffer(self):
        min_idx = min(self.proxies.values())
        if self.release_buffer is None or self.release_buffer.is_completed():
            # print('min idx %s' % min_idx)
            self.buffer.dequeue(min_idx - 1)

    def check_disposed(self):
        if self.is_disposed:
            raise DisposedException()

    def _subscribe_core(self, observer):
        class InnerSubscription:
            def __init__(self, subject, proxy):
                self.subject = subject
                self.proxy = proxy

                self.lock = config["concurrency"].RLock()

            def dispose(self):
                with self.lock:
                    if not self.subject.is_disposed and self.proxy:
                        if self.proxy in self.subject.proxies:
                            self.subject.proxies.pop(self.proxy, None)
                            self.proxy.dispose()
                        self.proxy = None

        def add_proxy(observer):
            with self.lock:
                # print('add proxy')
                first_idx = self.buffer.first_idx

                def update_source(proxy, idx):
                    with self.lock:
                        self.proxies[proxy] = idx
                    self._dequeue_buffer()

                proxy = BufferedSubject.ProxyBufferingFirst(buffer=self.buffer, last_idx=first_idx, observer=observer,
                                                            update_source=update_source)
                self.proxies[proxy] = first_idx
            self._request_source()
            return proxy

        with self.lock:
            self.check_disposed()
            if not self.is_stopped:
                proxy = add_proxy(observer=observer)
                observer.subscribe_backpressure(proxy)
                # print(observer)
                disposable = InnerSubscription(self, proxy)
                return disposable

            if self.exception:
                observer.on_error(self.exception)
                return Disposable.empty()

            observer.on_completed()
            return Disposable.empty()

    def on_completed(self):
        with self.lock:
            self.check_disposed()
            if not self.is_stopped:
                self._add_to_buffer(OnCompleted())

    def on_error(self, exception):
        """Notifies all subscribed observers with the exception.

        Keyword arguments:
        error -- The exception to send to all subscribed observers.
        """

        os = None
        with self.lock:
            self.check_disposed()
            if not self.is_stopped:
                # os = self.observers[:]
                # self.observers = []
                self.is_stopped = True
                self.exception = exception

        if os:
            for observer in os:
                observer.on_error(exception)

    def on_next(self, value):
        with self.lock:
            self.check_disposed()
            if not self.is_stopped:
                self._add_to_buffer(OnNext(value))

    def dispose(self):
        """Unsubscribe all observers and release resources."""

        # print('dispose called')
        with self.lock:
            self.is_disposed = True
            # self.observers = None