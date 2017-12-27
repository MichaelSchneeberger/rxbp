from rx import config
from rx.concurrency import current_thread_scheduler
from rx.core.notification import OnNext
from rx.internal import DisposedException

from rxbackpressure import BlockingFuture
from rxbackpressure.backpressuretypes.stoprequest import StopRequest


class BufferBackpressure():
    def __init__(self, buffer, last_idx, observer, update_source, dispose, scheduler=None):
        super().__init__()

        self.observer = observer
        self.buffer = buffer
        self.current_idx = last_idx
        self.requests = []
        self.is_stopped = False
        self._lock = config["concurrency"].RLock()
        self.dispose_func = dispose
        self.scheduler = scheduler or current_thread_scheduler
        self.is_disposed = False
        self.update_source = update_source

        observer.subscribe_backpressure(self)

    def check_disposed(self):
        if self.is_disposed:
            raise DisposedException()

    def request(self, number_of_items) -> BlockingFuture:
        future = BlockingFuture()

        def action(a, s):
            if not self.is_stopped:
                if isinstance(number_of_items, StopRequest):
                    self.dispose()
                    future.set(StopRequest)
                else:
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
        with self._lock:
            requests = self.requests
            self.requests = []

        def action(a, s):
            if requests:
                for future, _, __ in requests:
                    future.set(0)

        self.scheduler.schedule(action)

    def update(self) -> int:
        """ Sends buffered items to the observer

        :return: current buffer index
        """

        def take_requests_gen():
            """Returns an updated request list by checking the buffer

            :return:
            """
            for future, number_of_items, counter in self.requests:
                if self.current_idx < self.buffer.last_idx:
                    # there are new items in buffer

                    def get_value_from_buffer():
                        for _ in range(d_number_of_items):
                            value = self.buffer.get(self.current_idx)
                            self.current_idx += 1
                            yield value

                    if self.current_idx + number_of_items - counter <= self.buffer.last_idx:
                        # request fully fullfilled
                        d_number_of_items = number_of_items - counter
                        yield None, list(get_value_from_buffer()), (future, number_of_items)
                    else:
                        # request not fully fullfilled
                        d_number_of_items = self.buffer.last_idx - self.current_idx
                        yield (future, number_of_items, counter + d_number_of_items), list(
                            get_value_from_buffer()), None
                else:
                    # there are no new items in buffer
                    yield (future, number_of_items, counter), None, None

        if self.observer:
            # take as many requests as possible from self.requests
            has_elements = False
            with self._lock:
                if len(self.requests):
                    has_elements = True
                    request_list, buffer_value_list, future_tuple_list = zip(*take_requests_gen())
                    self.requests = [request for request in request_list if request is not None]

            # send values at some later time
            if has_elements is True:
                def action(a, s):

                    # set future from request
                    future_tuple_list_ = [e for e in future_tuple_list if e is not None]
                    for future, number_of_items in future_tuple_list_:
                        future.set(number_of_items)

                    # send values taken from buffer
                    value_to_send = [e for value_list in buffer_value_list if value_list is not None for e in
                                     value_list]
                    for value in value_to_send:
                        if isinstance(value, OnNext):
                            self.observer.on_next(value.value)
                        else:
                            self.is_stopped = True
                            self.observer.on_completed()
                            self._empty_requests()

                self.scheduler.schedule(action)

        # return current index in shared buffer
        return self.current_idx

    def dispose(self):
        with self._lock:
            self.is_disposed = True
            self.is_stopped = True
            self.requests = None
            self.observer = None
            self.dispose_func(self)