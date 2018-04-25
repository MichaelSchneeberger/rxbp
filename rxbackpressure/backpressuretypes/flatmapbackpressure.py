from rx import config, Observable
from rx.concurrency import immediate_scheduler
from rx.disposables import SingleAssignmentDisposable, RefCountDisposable
from rx.subjects import Subject

from rxbackpressure.backpressuretypes.stoprequest import StopRequest
from rxbackpressure.core.backpressurebase import BackpressureBase


class FlatMapBackpressure(BackpressureBase):
    def __init__(self, scheduler=None, backpressure=None):
        self.backpressure = backpressure
        self.scheduler = scheduler
        self.source_backpressure_list = []
        self.buffer = []
        self.requests = []
        self._lock = config["concurrency"].RLock()
        self.is_completed = False
        self.is_disposed = False

        # make sure that only one subflow is served at the time
        self.is_running = False

        self.disposable = SingleAssignmentDisposable()
        self.ref_count_disposable = RefCountDisposable(self)
        self.ref_count_disposable.is_primary_disposed = True

    def add_backpressure(self, backpressure):
        """ every element that a flatmap unit receives provides the backpressure interface

        :param backpressure:
        :param scheduler:
        :return:
        """

        self.source_backpressure_list.append(backpressure)
        self._request_source()
        disposable = self.ref_count_disposable.disposable
        return disposable

    def on_completed(self):
        # print('complete {}'.format(self))
        self.is_completed = True
        self._request_source()

    def request(self, number_of_items):
        # print('request flatmap {}'.format(number_of_items))

        if isinstance(number_of_items, StopRequest):
            self.is_completed = True

            # send stop request to backpressure and all sub backpressure
            # future = self.backpressure.request(number_of_items)
            # for backpressure in self.source_backpressure_list:
            #     backpressure.request(number_of_items)

            future = Observable.from_(self.source_backpressure_list, scheduler=immediate_scheduler) \
                .flat_map(lambda b: b.request(number_of_items)) \
                .merge(self.backpressure.request(number_of_items))
        else:
            future = Subject()
            self.requests.append((future, number_of_items))
            self._request_source()
        return future

    def dispose(self):
        self.is_disposed = True
        self.disposable.dispose()

    def _request_source(self):
        start_running = False
        complete_requests = False
        with self._lock:
            if not self.is_running and len(self.requests):
                if self.is_completed:
                    complete_requests = True
                    self.is_running = True
                elif len(self.source_backpressure_list):
                    start_running = True
                    self.is_running = True
                # if len(self.source_backpressure_list):
                #     start_running = True
                #     self.is_running = True
                # elif self.is_completed:
                #     complete_requests = True
                #     self.is_running = True

        # scheduler = self.scheduler or current_thread_scheduler

        # print(self.is_running)
        # print('{}, start={}, complete={}'.format(self, start_running, complete_requests))
        if complete_requests:
            for request in self.requests:
                future, number_of_items = request
                future.on_next(0)
                future.on_completed()

                # print(self.is_completed)
                # print('{} has backpressure list {}'.format(self, self.source_backpressure_list))

        elif start_running:
            def scheduled_action(_, __):
                future, number_of_items = self.requests[0]

                def handle_request(returned_num_of_items):
                    # print('2 returned_num_of_items = %s' % returned_num_of_items)
                    if returned_num_of_items < number_of_items:
                        # current source completed

                        # print('remove {}'.format(self.source_backpressure_list[0]))
                        self.source_backpressure_list.pop(0)
                        # self.source_backpressure_list = self.source_backpressure_list[1:]
                        d_number_of_items = number_of_items - returned_num_of_items
                        self.requests[0] = future, d_number_of_items
                    else:
                        # current request completed
                        self.requests.pop(0)
                        # self.requests = self.requests[1:]
                        # print('flatmap ack {}'.format(returned_num_of_items))
                        future.on_next(returned_num_of_items)
                        future.on_completed()
                    self.is_running = False
                    self._request_source()

                # backpressure of first child
                # print('request {}'.format(number_of_items))
                self.source_backpressure_list[0] \
                    .request(number_of_items) \
                    .subscribe(handle_request)

            # scheduler.schedule(scheduled_action)
            # print('request {}'.format(1))
            immediate_scheduler.schedule(scheduled_action)

    # def _request_source(self):
    #     start_running = False
    #     complete_requests = False
    #     with self._lock:
    #         if not self.is_running and len(self.requests):
    #             if len(self.source_backpressure_list):
    #                 start_running = True
    #                 self.is_running = True
    #             elif self.is_completed:
    #                 complete_requests = True
    #                 self.is_running = True
    #
    #     scheduler = self.scheduler or current_thread_scheduler
    #     # print(start_running)
    #     if start_running:
    #         def scheduled_action(a, s):
    #             future, number_of_items = self.requests[0]
    #
    #             def handle_request(returned_num_of_items):
    #                 # print('2 returned_num_of_items = %s' % returned_num_of_items)
    #                 if returned_num_of_items < number_of_items:
    #                     # current source completed
    #
    #                     self.source_backpressure_list.pop(0)
    #                     d_number_of_items = number_of_items - returned_num_of_items
    #                     self.requests[0] = future, d_number_of_items
    #                 else:
    #                     # current request completed
    #                     self.requests.pop(0)
    #                     # print('flatmap ack {}'.format(returned_num_of_items))
    #                     future.on_next(returned_num_of_items)
    #                     future.on_completed()
    #                 self.is_running = False
    #                 self._request_source()
    #
    #             # backpressure of first child
    #             self.source_backpressure_list[0] \
    #                 .request(number_of_items) \
    #                 .subscribe(handle_request)
    #
    #         scheduler.schedule(scheduled_action)
    #     elif complete_requests:
    #         def scheduled_action(a, s):
    #             for request in self.requests:
    #                 future, number_of_items = request
    #                 future.on_next(0)
    #                 future.on_completed()
    #
    #         scheduler.schedule(scheduled_action)
