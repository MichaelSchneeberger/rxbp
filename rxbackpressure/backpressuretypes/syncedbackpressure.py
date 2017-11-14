from rx import config
from rx.concurrency import immediate_scheduler

from rxbackpressure.core.backpressurebase import BackpressureBase
from rxbackpressure.internal.blockingfuture import BlockingFuture


class SyncedBackpressureProxy(BackpressureBase):
    def __init__(self, backpressure):
        self.backpressure = backpressure

    def request(self, number_of_items) -> BlockingFuture:
        future = self.backpressure.request(number_of_items, self)
        return future


class SyncedBackpressure:
    def __init__(self, scheduler=None):
        self.backpressure = None
        self.scheduler = scheduler or immediate_scheduler
        self.buffer = []
        self.requests = {}
        self.is_running = False
        self._lock = config["concurrency"].RLock()

    def add_proxy(self, proxy):
        with self._lock:
            # print('add observer %s' % observer)
            self.requests[proxy] = []
            return proxy
        self._request_source()

    def add_backpressure(self, backpressure):
        # print('add backpressure')
        with self._lock:
            if self.backpressure is None:
                self.backpressure = backpressure
        self._request_source()

    def request(self, number_of_items, proxy) -> BlockingFuture:
        # print('1 request received, num = %s' %number_of_items)
        future = BlockingFuture()
        self.requests[proxy].append((future, number_of_items, 0))
        self._request_source()
        return future

    def _request_source(self):
        send_item = False
        with self._lock:
            if self.backpressure is not None \
                    and not self.is_running \
                    and len(self.requests) \
                    and all(len(e) for e in self.requests.values()):
                self.is_running = True
                send_item = True

        if send_item:
            def scheduled_action(a, s):
                # print(self.requests)
                number_of_items_list = [sum(e[1] for e in list1) for list1 in self.requests.values()]
                number_of_items = min(number_of_items_list)

                future = self.backpressure.request(number_of_items)
                def update_request(v):
                    # print('zero received')
                    self.is_running = False
                    if v == 0:
                        for proxy, list1 in self.requests.items():
                            for request in list1:
                                future, req_num_of_items, counter = request
                                future.set(0)
                                self.requests[proxy].pop(0)
                    else:
                        for _ in range(v):
                            for proxy, list1 in self.requests.items():
                                future, req_num_of_items, counter = list1[0]
                                counter += 1
                                if req_num_of_items == counter:
                                    future.set(req_num_of_items)
                                    self.requests[proxy].pop(0)
                                else:
                                    self.requests[proxy][0][3] = counter
                        self._request_source()
                future.map(update_request)

            self.scheduler.schedule(scheduled_action)