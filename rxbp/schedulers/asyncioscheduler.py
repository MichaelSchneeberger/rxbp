import asyncio
import datetime
from threading import Thread
from typing import Union, Callable, Any

from rx.disposable import Disposable

from rxbp.scheduler import SchedulerBase


class AsyncIOScheduler(SchedulerBase, Disposable):
    def __init__(self, loop: asyncio.AbstractEventLoop = None, new_thread: bool = None):
        super().__init__()

        self.loop: asyncio.AbstractEventLoop = loop or asyncio.new_event_loop()

        if new_thread is None or new_thread is True:
            t = Thread(target=self.start_loop)
            t.setDaemon(True)
            t.start()

    def start_loop(self):
        asyncio.set_event_loop(self.loop)
        self.loop.run_forever()

    @property
    def now(self):
        return self.loop.time()

    def schedule(self,
                 action: Callable[[SchedulerBase, Any], None],
                 state=None):
        def func():
            action(self, state)

        handle = self.loop.call_soon_threadsafe(func)

        def dispose():
            handle.cancel()

        return Disposable(dispose)

    def schedule_relative(self,
                          duetime: Union[int, float],
                          action: Callable[[SchedulerBase, Any], None],
                          state=None):

        if isinstance(duetime, datetime.datetime):
            timespan = duetime - datetime.datetime.fromtimestamp(0)
            timespan = int(timespan.total_seconds())
        elif isinstance(duetime, datetime.timedelta):
            timespan = int(duetime.total_seconds())
        else:
            timespan = duetime

        def _():
            def func():
                action(self, state)

            self.loop.call_later(timespan, func)

        handle = self.loop.call_soon_threadsafe(_)

        def dispose():
            handle.cancel()

        return Disposable(dispose)

    def schedule_absolute(self,
                          duetime: datetime.datetime,
                          func: Callable[[SchedulerBase, Any], None],
                          state=None):
        timedelta = (duetime - datetime.datetime.now()).total_seconds()
        return self.schedule_relative(timedelta, func)

    def dispose(self):
        def _():
            self.loop.stop()

        self.loop.call_soon_threadsafe(_)
