import copy
import queue
import sys
import traceback
from threading import Lock

from rx import Observable
from rx.concurrency import immediate_scheduler, current_thread_scheduler


# todo: add type hint
class BlockingFuture:
    def __init__(self, scheduler=None):
        super().__init__()

        self._queue = queue.Queue(maxsize=1)
        self._data = None
        self._done_hook_list = []
        self._on_error_hook_list = []
        self._lock = Lock()
        self._scheduler = scheduler or immediate_scheduler

    @staticmethod
    def reraise(tp, value, tb=None):
        if value is None:
            value = tp()
        if value.__traceback__ is not tb:
            raise value.with_traceback(tb)
        raise value

    def is_completed(self):
        """
        Return whether the future has already been completed with a value or an exception.

        :return: 
        """
        return self._data is not None or self._queue.full()

    def set(self, value=None):
        return self.set_result(value)

    def set_result(self, value=None):

        with self._lock:
            if self._queue.empty():
                v1 = {'value': value}
                self._queue.put(item=v1, block=False)

        for done_hook in self._done_hook_list:
            done_hook(self)

    def set_exception(self, exc_info=None):
        exc_info_ = (type(exc_info), exc_info) if exc_info else sys.exc_info()

        with self._lock:
            # print('put exception %s' % type(sys.exc_info()[0]))
            self._queue.put({'exc_info': exc_info_})

        for on_error_hook in self._on_error_hook_list:
            on_error_hook()

    def _read_queue(self, timeout=None):
        if self._data is None:
            self._data = self._queue.get(block=True, timeout=timeout)
        return self._data

    def get(self, timeout=None):
        return self.result(timeout=timeout)

    def result(self, timeout=None):
        try:
            data = self._read_queue()

            if 'exc_info' in data:
                # print(data['exc_info'])
                self.reraise(*data['exc_info'])
            else:
                return data['value']
        except queue.Empty:
            raise NameError('%s seconds' % timeout)

    def get_exception(self):
        return self._read_queue()['exc_info']

    def add_on_error_callback(self, on_error):
        def schedule_action():
            exc_info = self.get_exception()
            Observable.just(None, self._scheduler).subscribe(lambda v: on_error(exc_info))

        schedule_action_ = lambda: None
        with self._lock:
            if self.is_completed() and 'exc_info' in self._read_queue():
                schedule_action_ = schedule_action
            else:
                self._on_error_hook_list.append(schedule_action)

        schedule_action_()

    def add_done_callback(self, done):
        # cb_future = BlockingFuture(self._scheduler)

        def schedule_action(future):
            # Observable.just(None, self._scheduler).subscribe(lambda v: cb_future.set(done(future)))
            Observable.just(None, self._scheduler).subscribe(lambda v: done(future))

        schedule_action_ = lambda f: None
        with self._lock:
            if self.is_completed() and 'value' in self._read_queue():
                schedule_action_ = schedule_action
            else:
                self._done_hook_list.append(schedule_action)

        schedule_action_(self)

    def subscribe(self, func):
        def done(future):
            func(future.get())
        self.add_done_callback(done)

    def map(self, func) -> 'BlockingFuture':
        result_future = BlockingFuture(self._scheduler)

        def done(future):
            try:
                # print('future %s' % future)
                # print('future result %s' % future.result())
                v1 = func(future.result())
                result_future.set(v1)
            except:
                result_future.set_exception()

        def on_error_func(exc_info):
            result_future.set_exception(exc_info)

        self.add_done_callback(done)
        self.add_on_error_callback(on_error_func)
        return result_future

    def flat_map(self, func) -> 'Blocking Future':
        return self.map(func).flatten()

    def fallback_to(self, fallback_future) -> 'BlockingFuture':
        """
        if future fails then fallback to result from fallback_future

        :param fallback_future:
        :return:
        """

        result_future = BlockingFuture(self._scheduler)

        def done(future):
            v1 = future.result()
            result_future.set(v1)

        def on_error_func(exc_info):
            def on_error_func_2(fall_back_exc_info):
                result_future.set_exception(fall_back_exc_info)

            fallback_future.add_done_callback(done)
            fallback_future.add_on_error_callback(on_error_func_2)

        self.add_done_callback(done)
        self.add_on_error_callback(on_error_func)
        return result_future

    def recover(self, func) -> 'BlockingFuture':
        """
        if future fails then execute func and return its value

        :param func:
        :return:
        """
        result_future = BlockingFuture(self._scheduler)

        def done(future):
            v1 = future.result()
            result_future.set(v1)

        def on_error_func(exc_info):
            result_future.set(func(exc_info))

        self.add_done_callback(done)
        self.add_on_error_callback(on_error_func)
        return result_future

    def zip(self, other, selector=None):
        result_future = BlockingFuture(self._scheduler)

        def merge(v1, v2):
            v_zip = selector(v1, v2) if selector else [v1, v2]
            result_future.set(v_zip)

        def done1(future):
            v1 = future.get()
            other.add_done_callback(lambda f2: merge(v1, f2.get()))

        def on_error_func(exc_info):
            result_future.set_exception(exc_info)

        self.add_on_error_callback(on_error_func)
        other.add_on_error_callback(on_error_func)
        self.add_done_callback(done1)

        return result_future

    def flatten(self) -> 'BlockingFuture':
        result_future = BlockingFuture(self._scheduler)

        def on_error_func(exc_info):
            result_future.set_exception(exc_info)

        def flatten_hook(future):
            v = future.result()
            if isinstance(v, BlockingFuture):
                inner_future = v  # type: BlockingFuture
                inner_future.add_done_callback(flatten_hook)
                inner_future.add_on_error_callback(on_error_func)
            else:
                result_future.set(v)

        self.add_done_callback(flatten_hook)
        self.add_on_error_callback(on_error_func)
        return result_future

    def cancel(self):
        pass
