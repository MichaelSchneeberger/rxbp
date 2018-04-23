import math

from rx import config, Observer
from rx.concurrency import current_thread_scheduler, immediate_scheduler
from rx.core import Disposable
from rx.core.notification import OnNext, OnCompleted
from rx.internal import DisposedException

from rxbackpressure.backpressuretypes.bufferbackpressure import BufferBackpressure
from rxbackpressure.buffers.dequeuablebuffer import DequeuableBuffer
from rxbackpressure.core.backpressureobservable import BackpressureObservable


class BufferedSubject(BackpressureObservable, Observer):

    def __init__(self, name=None, scheduler=None, release_buffer=None):
        super().__init__()

        self.name = name
        self.scheduler = scheduler or immediate_scheduler
        self.is_disposed = False
        self.is_stopped = False
        self.proxies = {}
        self.buffer = DequeuableBuffer()
        self.exception = None
        self.release_buffer = release_buffer

        if release_buffer:
            release_buffer.subscribe(lambda v: self.request_source())

        self.lock = config["concurrency"].RLock()

    def _add_to_buffer(self, v):
        # print('add to buffer %s' % v)
        if not self.is_disposed:
            self.buffer.append(v)
            if self.proxies:
                self.request_source()
        else:
            print('disposed')

    def request_source(self):
        def scheduled_action(a, s):
            if self.proxies and len(self.proxies.items()):
                with self.lock:
                    for proxy in self.proxies.keys():
                        self.proxies[proxy] = proxy.update()
            self._dequeue_buffer()

        self.scheduler.schedule(scheduled_action)

    def _dequeue_buffer(self):
        try:
            min_idx = min(self.proxies.values())
        except ValueError:
            min_idx = math.inf

        # if (self.release_buffer is None or self.release_buffer.is_completed()) > 0:
        if (self.release_buffer is None or self.release_buffer.has_value) and len(self.proxies) > 0:
            self.buffer.dequeue(min_idx - 1)

    def check_disposed(self):
        if self.is_disposed:
            raise DisposedException()

    def _subscribe_core(self, observer, scheduler=None):
        # print('subscribe')
        class InnerSubscription:
            def __init__(self, subject, proxy):
                self.subject = subject
                self.proxy = proxy

                self.lock = config["concurrency"].RLock()

            def dispose(self):
                # print('dispose')
                with self.lock:
                    if not self.subject.is_disposed and self.proxy:
                        if self.proxy in self.subject.proxies:
                            # self.subject.proxies.pop(self.proxy, None)
                            # print('dipose proxy')
                            self.proxy.dispose()
                            self.subject.request_source()
                        self.proxy = None

        def add_proxy(observer):
            with self.lock:
                # print('add proxy')
                first_idx = self.buffer.first_idx

                def update_source(proxy, idx):
                    with self.lock:
                        self.proxies[proxy] = idx
                    self._dequeue_buffer()

                def remove_proxy(proxy):
                    with self.lock:
                        # print('remove proxy {}'.format(proxy))
                        if proxy in self.proxies:
                            self.proxies.pop(proxy)

                # self.scheduler = self.scheduler or scheduler or current_thread_scheduler

                proxy = BufferBackpressure(buffer=self.buffer,
                                           last_idx=first_idx,
                                           observer=observer,
                                           update_source=update_source,
                                           dispose=remove_proxy)
                self.proxies[proxy] = first_idx
            self.request_source()
            return proxy

        with self.lock:
            self.check_disposed()
            if not self.is_stopped:
                proxy = add_proxy(observer=observer)
                disposable = InnerSubscription(self, proxy)
                return disposable

            if self.exception:
                observer.on_error(self.exception)
                return Disposable.empty()

            observer.on_completed()
            return Disposable.empty()

    def on_next(self, value):
        with self.lock:
            self.check_disposed()
            if not self.is_stopped:
                self._add_to_buffer(OnNext(value))

    def on_completed(self):
        with self.lock:
            self.check_disposed()
            if not self.is_stopped:
                self._add_to_buffer(OnCompleted())

    def on_error(self, exception):
        """Notifies all subscribed obserelease_bufferrvers with the exception.

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

    def dispose(self):
        """Unsubscribe all observers and release resources."""

        with self.lock:
            # print('diposed')
            self.is_disposed = True
            self.proxies = None