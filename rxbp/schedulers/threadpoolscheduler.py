import asyncio
import atexit
import concurrent
import datetime
import time
from concurrent.futures import Executor
from concurrent.futures.thread import ThreadPoolExecutor

from rx.core.typing import RelativeTime
from rx.disposable import Disposable, MultipleAssignmentDisposable, CompositeDisposable

from rxbp.schedulers.asyncioscheduler import AsyncIOScheduler


class ThreadPoolScheduler(AsyncIOScheduler):

    def __init__(self, name, loop: asyncio.AbstractEventLoop = None, new_thread=True, executor: Executor = None,
                 max_workers = None):

        # starts a new thread
        super().__init__(loop=loop, new_thread=new_thread)

        self.executor = executor or ThreadPoolExecutor(max_workers=max_workers)

        # closes daemon threads after _main thread terminated
        # https://stackoverflow.com/questions/48350257/how-to-exit-a-script-after-threadpoolexecutor-has-timed-out
        atexit.unregister(concurrent.futures.thread._python_exit)   # type: ignore
        self.executor.shutdown = lambda wait: None                  # type: ignore

        self.disposable = None

    @property
    def is_order_guaranteed(self) -> bool:
        return False

    def sleep(self, seconds: float) -> None:
        time.sleep(seconds)

    def schedule(self, action, state=None):
        # def outer_action(_, __):
        def func():
            action(self, None)

        future = self.executor.submit(func)

        def dispose():
            future.cancel()

        disposable = Disposable(dispose)
        return disposable
        # super().schedule(outer_action)

    def schedule_relative(
            self,
            duetime: RelativeTime,
            action,
            state=None,
    ):
        if isinstance(duetime, datetime.datetime):
            timedelta = duetime - datetime.datetime.fromtimestamp(0)
            timespan = float(timedelta.total_seconds())
        elif isinstance(duetime, datetime.timedelta):
            timespan = float(duetime.total_seconds())
        else:
            timespan = duetime

        def func():
            action(self, None)

        disposable = [MultipleAssignmentDisposable()]

        def _():
            def __():
                future = self.executor.submit(func)
                disposable[0] = Disposable(lambda: future.cancel())
            self.loop.call_later(timespan, __)

        future = self.loop.call_soon_threadsafe(_)
        return CompositeDisposable(disposable, Disposable(lambda: future.cancel()))

    def schedule_absolute(
            self,
            duetime,
            action,
            state=None,
    ):
        timedelta = (duetime - datetime.datetime.now()).total_seconds()
        return self.schedule_relative(timedelta, action)

    def dispose(self):
        super().dispose()