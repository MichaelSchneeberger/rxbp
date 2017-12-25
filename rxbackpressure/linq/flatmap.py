from rx.concurrency import immediate_scheduler
from rx.disposables import CompositeDisposable, SingleAssignmentDisposable
from rx.internal import extensionmethod

from rxbackpressure.backpressuretypes.backpressuregreadily import \
    BackpressureGreadily
from rxbackpressure.core.anonymousbackpressureobservable import \
    AnonymousBackpressureObservable
from rxbackpressure.core.backpressurebase import BackpressureBase
from rxbackpressure.core.backpressureobservable import BackpressureObservable
from rxbackpressure.internal.blockingfuture import BlockingFuture


@extensionmethod(BackpressureObservable)
def flat_map(self, selector):

    class FlatMapBackpressure(BackpressureBase):
        def __init__(self, scheduler=None):
            self.scheduler = scheduler or immediate_scheduler
            self.source_backpressure_list = []
            self.buffer = []
            self.requests = []
            self.is_running = False
            # self.observer

        def add_backpressure(self, backpressure):
            # print('add backpressure')
            self.source_backpressure_list.append(backpressure)
            self._request_source()

        # def add_to_buffer(self, v):
        #     self.buffer.append(v)
        #     self._request_source()

        def request(self, number_of_items) -> BlockingFuture:
            # print('request received, num = %s' %number_of_items)
            future = BlockingFuture()
            self.requests.append((future, number_of_items))
            self._request_source()
            return future

        def _request_source(self):
            # if not len(self.active_sources):
            #     def scheduled_action(a, s):
            #         future, number_of_items = self.requests.pop(0)
            #         future.set(0)
            #     self.scheduler.schedule(scheduled_action)
            if not self.is_running and len(self.requests) and len(self.source_backpressure_list):
                # print('send')
                self.is_running = True

                def scheduled_action(a, s):
                    future, number_of_items = self.requests[0]

                    def handle_request(returned_num_of_items):
                        # print('2 returned_num_of_items = %s' % returned_num_of_items)
                        if returned_num_of_items < number_of_items:
                            self.source_backpressure_list.pop(0)
                            d_number_of_items = number_of_items - returned_num_of_items
                            self.requests[0] = future, d_number_of_items
                        else:
                            self.requests.pop(0)
                            future.set(returned_num_of_items)
                        self.is_running = False
                        self._request_source()

                    # backpressure of first child
                    self.source_backpressure_list[0].request(number_of_items) \
                        .map(handle_request)

                self.scheduler.schedule(scheduled_action)

    sub_backpressure = FlatMapBackpressure()

    def subscribe_func(observer):
        # count = [0]
        group = CompositeDisposable()
        is_stopped = [False]
        m = SingleAssignmentDisposable()
        group.add(m)

        def on_next(value):
            try:
                source = selector(value)
                # source = selector(value, count[0])
                # sub_backpressure.add_source(source)
            except Exception as err:
                observer.on_error(err)
            else:
                inner_subscription = SingleAssignmentDisposable()
                group.add(inner_subscription)

                def on_completed():
                    group.remove(inner_subscription)
                    if is_stopped[0] and len(group) == 1:
                        observer.on_completed()

                # count[0] += 1
                def subscribe_bp_func(backpressure):
                    # print('subscribe backpressure to inner element')
                    sub_backpressure.add_backpressure(backpressure)

                def on_next(v):
                    observer.on_next(v)

                source.subscribe(subscribe_bp=subscribe_bp_func, on_next=on_next, on_error=observer.on_error, on_completed=on_completed)

        def on_completed():
            # print('on complete')
            is_stopped[0] = True
            if len(group) == 1:
                observer.on_completed()

        observer.subscribe_backpressure(sub_backpressure)

        def subscribe_bp_func(backpressure):
            BackpressureGreadily.apply(backpressure)

        m.disposable = self.subscribe(subscribe_bp=subscribe_bp_func,
                                      on_next=on_next, on_error=observer.on_error, on_completed=on_completed)
        # print(group)
        return group

        # def subscribe_func(observer):
        #
        #     def inner_subscribe(inner_observer):
        #         # print('inner observer %s' % inner_observer)
        #         subscribe_func(inner_observer)
        #
        #     def modified_selector(value, idx):
        #         # print('value %s received' % value)
        #         obs1 = selector(value, idx)
        #         # backpressure, subscribe_func = obs1._get_backpressure()
        #         # sub_backpressure.add_backpressure(backpressure)
        #         return AnonymousObservable(subscribe_func)
        #
        #     def subscribe_bp_func(backpressure):
        #         BackpressureGreadily.apply(backpressure)
        #
        #     observer = AnonymousObservable(subscribe=inner_subscribe) \
        #         .flat_map(modified_selector) \
        #         .subscribe(observer)
        #
        #     self.subscribe(subscribe_bp=subscribe_bp_func, observer=observer)
        #
    return AnonymousBackpressureObservable(subscribe_func=subscribe_func)
