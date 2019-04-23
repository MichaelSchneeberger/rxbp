import asyncio
import atexit
import concurrent
import datetime
from concurrent.futures import Executor
from concurrent.futures.thread import ThreadPoolExecutor
from typing import Callable, Union, Any

from rx.disposable import Disposable, Scheduler
from rx.disposables import MultipleAssignmentDisposable, CompositeDisposable

from rxbp.schedulers.asyncioscheduler import AsyncIOScheduler


class ThreadPoolScheduler(AsyncIOScheduler):

    def __init__(self, loop: asyncio.AbstractEventLoop = None, new_thread=True, executor: Executor = None,
                 max_workers = None):
        self.executor = executor or ThreadPoolExecutor(max_workers=max_workers)
        # terminate when main thread terminates
        # https://stackoverflow.com/questions/48350257/how-to-exit-a-script-after-threadpoolexecutor-has-timed-out
        atexit.unregister(concurrent.futures.thread._python_exit)
        self.executor.shutdown = lambda wait: None

        super().__init__(loop, new_thread)

    def schedule(self, action: Callable[[Scheduler, Any], None], state=None):
        def func():
            action(self, state)

        future = self.executor.submit(func)

        def dispose():
            future.cancel()

        return Disposable(dispose)

    def schedule_relative(self,
                          timedelta: Union[int, float],
                          action: Callable[[Scheduler, Any], None],
                          state=None):
        disposable = [MultipleAssignmentDisposable()]

        def _():
            def __():
                def func():
                    action(self, state)

                future = self.executor.submit(func)
                disposable[0] = Disposable(lambda: future.cancel())
            self.loop.call_later(timedelta, __)

        future = self.loop.call_soon_threadsafe(_)
        # super().schedule_relative(timedelta, __)
        return CompositeDisposable(disposable, Disposable(lambda: future.cancel()))

    def schedule_absolute(self,
                          duetime: datetime.datetime,
                          action: Callable[[Scheduler, Any], None],
                          state=None):
        timedelta = (duetime - datetime.datetime.now()).total_seconds()
        return self.schedule_relative(timedelta, func)
