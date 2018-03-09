import math
from rx import config
from rx.concurrency import immediate_scheduler, current_thread_scheduler
from rx.core import Disposable
from rx.disposables import CompositeDisposable
from rx.subjects import AsyncSubject, Subject

from rxbackpressure.backpressuretypes.stoprequest import StopRequest
from rxbackpressure.core.backpressurebase import BackpressureBase


class SyncedBackpressureProxy(BackpressureBase):
    def __init__(self, backpressure):
        self.backpressure = backpressure

    def request(self, number_of_items):
        future = self.backpressure.request(number_of_items, self)
        return future


class SyncedBackpressure:
    def __init__(self, scheduler=None, release=None):
        self.backpressure = None
        self.scheduler = scheduler
        self.buffer = []
        self.requests = {}
        self.is_running = False
        self._lock = config["concurrency"].RLock()
        self.release_signal = release
        self.is_disposed = False

        if release is not None:
            release.subscribe(lambda v: self._request_source())

    def add_observer(self, observer, scheduler):
        # print(observer.observer)
        proxy_backpressure = SyncedBackpressureProxy(backpressure=self)
        with self._lock:
            # print('add observer %s' % observer)
            self.requests[proxy_backpressure] = []
        self._request_source()
        self.scheduler = self.scheduler or scheduler
        disposable = observer.subscribe_backpressure(proxy_backpressure, self.scheduler)
        # self.child_disposable.add(disposable)
        return disposable

    def add_backpressure(self, backpressure, scheduler):
        # called once
        with self._lock:
            if self.backpressure is None:
                self.backpressure = backpressure
        self.scheduler = self.scheduler or scheduler
        self._request_source()
        return Disposable.empty()

    def dispose(self):
        # print('dispose sync')
        self.is_disposed = True
        self.child_disposable.dispose()

    def request(self, number_of_items, proxy):
        # print('1 request received, num = %s' %number_of_items)
        future = Subject()
        self.requests[proxy].append((future, number_of_items, 0))
        if self.release_signal is None or self.release_signal.has_value:
            self._request_source()
        return future

    def _request_source(self):
        send_item = False
        with self._lock:
            if self.backpressure is not None \
                    and not self.is_running \
                    and len(self.requests) > 0 \
                    and all(len(e) for e in self.requests.values()):
                self.is_running = True
                send_item = True

        if send_item:
            def scheduled_action(a, s):

                def update_request(v):
                    self.is_running = False

                    for _ in range(v):
                        def gen1():
                            for proxy, list1 in self.requests.items():
                                future, req_num_of_items, counter = list1[0]

                                if isinstance(req_num_of_items, StopRequest):
                                    future.on_next(req_num_of_items)
                                    future.on_completed()
                                else:
                                    counter += v
                                    if req_num_of_items == counter:
                                        future.on_next(req_num_of_items)
                                        future.on_completed()
                                        list1.pop(0)
                                    else:
                                        list1[0][3] = counter
                                    yield proxy, list1
                        self.requests = dict(gen1())
                        if not self.requests:
                            self.backpressure.request(StopRequest())

                    if v < number_of_items:
                        # source couldn't serve request, delete all requests
                        for proxy, list1 in self.requests.items():
                            for request in list1:
                                future, req_num_of_items, counter = request
                                future.on_next(0)
                                future.on_completed()
                        self.requests = {}
                    else:
                        # check if next request is a stop request
                        def gen1():
                            for proxy, list1 in self.requests.items():
                                if list1:
                                    future, req_num_of_items, counter = list1[0]
                                    if isinstance(req_num_of_items, StopRequest):
                                        future.on_next(req_num_of_items)
                                        future.on_completed()
                                    else:
                                        yield proxy, list1
                                else:
                                    yield proxy, list1
                        self.requests = dict(gen1())
                        if not self.requests:
                            self.backpressure.request(StopRequest())

                        self.is_running = False
                        self._request_source()

                number_of_items_list = [sum(e[1] for e in list1) for list1 in self.requests.values()]
                number_of_items = min(number_of_items_list)

                if number_of_items != math.inf:
                    self.backpressure.request(number_of_items).subscribe(update_request)
                else:
                    number_of_items_list = [sum(e[1] for e in list1 if not isinstance(e[1], StopRequest)) for list1 in self.requests.values()]
                    number_of_items = max(number_of_items_list)
                    update_request(0)

            scheduler = self.scheduler or current_thread_scheduler
            scheduler.schedule(scheduled_action)