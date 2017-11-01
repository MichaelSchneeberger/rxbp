from rx import config
from rx.concurrency import current_thread_scheduler
from rx.disposables import CompositeDisposable
from rx.internal import extensionmethod

from rx_backpressure.core.anonymous_backpressure_observable import \
    AnonymousBackpressureObservable
from rx_backpressure.core.backpressure_base import BackpressureBase
from rx_backpressure.core.backpressure_observable import BackpressureObservable
from rx_backpressure.internal.blocking_future import BlockingFuture
from rx_backpressure.subjects.synced_backpressure_subject import SyncedBackpressureSubject


@extensionmethod(BackpressureObservable)
def window(self, other, is_lower, is_higher):
    source = self

    class WindowBackpressure(BackpressureBase):
        def __init__(self, backpressure):
            self.backpressure = backpressure

            self._lock = config["concurrency"].RLock()
            self.scheduler = current_thread_scheduler
            self.requests = []

        def request(self, number_of_items):
            future = BlockingFuture()
            def action(a, s):
                is_first = False
                with self._lock:
                    if len(self.requests) == 0:
                        is_first = True
                    self.requests.append((future, number_of_items, 0))
                if is_first:
                    self.backpressure.request(number_of_items)
            self.scheduler.schedule(action)
            return future

        def update(self):
            def action(a, s):
                future, number_of_items, current_number = self.requests[0]
                new_request = (future, number_of_items, current_number + 1)
                if new_request[2] == number_of_items:
                    # print('set future')
                    future.set(number_of_items)
                    has_requests = False
                    with self._lock:
                        self.requests.pop()
                        if len(self.requests) > 0:
                            has_requests = True
                    if has_requests:
                        future, number_of_items, current_number = self.requests[0]
                        self.backpressure.request(number_of_items)
                else:
                    with self._lock:
                        self.requests[0] = new_request
            self.scheduler.schedule(action)

    class ElementBackpressure(BackpressureBase):
        def __init__(self, backpressure):
            self.backpressure = backpressure

            self._lock = config["concurrency"].RLock()
            self.scheduler = current_thread_scheduler
            self.requests = []
            self.current_request = None
            self.num_elements_removed = 0
            self.num_elements_req = 0

        def request(self, number_of_items):
            # print('request arrived')
            future = BlockingFuture()
            self.requests.append((future, number_of_items, 0))
            self.update()
            return future

        def update(self):
            def action(a, s):
                open_new_request = False
                with self._lock:
                    # print(len(self.requests))
                    if self.current_request is None and len(self.requests) > 0:
                        open_new_request = True
                        self.current_request = self.requests.pop()
                if open_new_request:
                    future, number_of_items, current = self.current_request
                    delta = self.num_elements_req - number_of_items
                    # print(delta)
                    if delta < 0:
                        self.num_elements_req = 0
                        self.backpressure.request(-delta)
                    else:
                        self.num_elements_req = delta
            self.scheduler.schedule(action)

        def update_current_request(self):
            future, num_of_items, current_num = self.current_request
            if current_num + self.num_elements_removed == num_of_items:
                if self.num_elements_removed > 0:
                    self.backpressure.request(self.num_elements_removed)
                    self.num_elements_removed = 0
                else:
                    future.set(num_of_items)
                    self.current_request = None
                    self.update()

        def remove_element(self, num=1):
            # print('remove')
            self.num_elements_removed += num
            self.update_current_request()

        def next_element(self, num=1):
            if self.current_request:
                future, num_of_items, current_num = self.current_request
                current_num += num
                self.current_request = (future, num_of_items, current_num)
                self.update_current_request()

        def finish_current_request(self):
            if self.current_request is not None:
                with self._lock:
                    self.requests = []
                    future, num_of_items, current_num = self.current_request
                    future.set(current_num)
                    self.num_elements_req = num_of_items - current_num
                    self.current_request = None

    def subscribe_func(observer):
        lock = config["concurrency"].RLock()
        element_list = []
        opening_list = []
        scheduler = current_thread_scheduler
        current_subject = [None]
        backpressure = [None]
        element_backpressure = [None]

        def start_process():
            def action(a, s):
                element = element_list[0]
                opening = opening_list[0]

                # print(is_lower(opening, element))
                # print(element)
                # print(opening)

                if is_lower(opening, element):
                    # remove first element
                    element_list.pop(0)

                    element_backpressure[0].remove_element()
                elif is_higher(opening, element):
                    # complete subject
                    current_subject[0].on_completed()
                    current_subject[0] = None
                    # remove first opening
                    opening_list.pop(0)

                    element_backpressure[0].finish_current_request()
                    backpressure[0].update()
                else:
                    # send element to inner subject
                    # print('on next')
                    current_subject[0].on_next(element)

                    # remove first element
                    element_list.pop(0)

                    element_backpressure[0].next_element()

                with lock:
                    if len(opening_list) > 0 and len(element_list) > 0:
                        start_process()

            scheduler.schedule(action)

        def on_next_opening(value):
            # print('opening received value=%s' % list(value))

            with lock:
                if current_subject[0] is None:
                    current_subject[0] = SyncedBackpressureSubject()
                    current_subject[0].subscribe_backpressure(element_backpressure[0])
                    observer.on_next((value, current_subject[0]))
                    # print('subscribe now')

                if len(opening_list) == 0 and len(element_list) > 0:
                    opening_list.append(value)
                    start_process()
                else:
                    opening_list.append(value)

        def on_next_element(value):
            # print('element received value')
            # print(value)
            with lock:
                if len(element_list) == 0 and len(opening_list) > 0:
                    element_list.append(value)
                    start_process()
                else:
                    # len(element_list) > 0, then the process has already been started
                    element_list.append(value)

        def subscribe_pb_opening(parent_backpressure):
            # print('subscribe opening backpressure')
            backpressure[0] = WindowBackpressure(parent_backpressure)
            observer.subscribe_backpressure(backpressure[0])
            return backpressure[0]

        def subscribe_pb_element(backpressure):
            element_backpressure[0] = ElementBackpressure(backpressure)
            # element_backpressure[0] = backpressure

        def on_completed():
            with lock:
                if current_subject[0]:
                    current_subject[0].on_completed()
            observer.on_completed()

        d1 = other.subscribe(on_next=on_next_element, on_completed=on_completed, on_error=observer.on_error,
                        subscribe_bp=subscribe_pb_element)
        d2 = source.subscribe(on_next=on_next_opening, on_completed=on_completed, on_error=observer.on_error,
                         subscribe_bp=subscribe_pb_opening)
        return CompositeDisposable(d1, d2)

    obs = AnonymousBackpressureObservable(subscribe_func=subscribe_func)
    return obs
