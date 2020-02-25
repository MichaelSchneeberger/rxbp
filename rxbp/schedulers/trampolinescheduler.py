import datetime
import logging
import threading
import time
from typing import Optional

from rx.core import typing
from rx.internal import PriorityQueue
from rx.scheduler.scheduleditem import ScheduledItem
from rx.scheduler.scheduler import Scheduler

from rxbp.scheduler import SchedulerBase as RxBPSchedulerBase

log = logging.getLogger('Rx')


class TrampolineScheduler(RxBPSchedulerBase, Scheduler):
    def __init__(self):
        """Gets a scheduler that schedules work as soon as possible on the
        current thread."""

        super().__init__()

        self.idle = True
        self.queue = PriorityQueue()

        self.lock = threading.RLock()

    class Trampoline(object):
        @classmethod
        def run(cls, queue: PriorityQueue) -> None:
            while queue:
                item: ScheduledItem = queue.peek()
                if item.is_cancelled():
                    queue.dequeue()
                else:
                    diff = item.duetime - item.scheduler.now
                    if diff <= datetime.timedelta(0):
                        item.invoke()
                        queue.dequeue()
                    else:
                        time.sleep(diff.total_seconds())

    def sleep(self, seconds: float) -> None:
        time.sleep(seconds)

    @property
    def is_order_guaranteed(self) -> bool:
        return True

    def schedule(self,
                 action: typing.ScheduledAction,
                 state: Optional[typing.TState] = None
                 ) -> typing.Disposable:
        """Schedules an action to be executed.
        Args:
            action: Action to be executed.
            state: [Optional] state to be given to the action function.
        Returns:
            The disposable object used to cancel the scheduled action
            (best effort).
        """

        return self.schedule_absolute(self.now, action, state=state)

    def schedule_relative(self,
                          duetime: typing.RelativeTime,
                          action: typing.ScheduledAction,
                          state: Optional[typing.TState] = None
                          ) -> typing.Disposable:
        """Schedules an action to be executed after duetime.
        Args:
            duetime: Relative time after which to execute the action.
            action: Action to be executed.
            state: [Optional] state to be given to the action function.
        Returns:
            The disposable object used to cancel the scheduled action
            (best effort).
        """

        # duetime = SchedulerBase.normalize(self.to_timedelta(duetime))
        duetime = self.to_timedelta(duetime)
        return self.schedule_absolute(self.now + duetime, action, state=state)

    def schedule_absolute(self, duetime: typing.AbsoluteTime,
                          action: typing.ScheduledAction,
                          state: Optional[typing.TState] = None
                          ) -> typing.Disposable:
        """Schedules an action to be executed at duetime.
        Args:
            duetime: Absolute time after which to execute the action.
            action: Action to be executed.
            state: [Optional] state to be given to the action function.
        """

        duetime = self.to_datetime(duetime)

        if duetime > self.now:
            log.warning("Do not schedule imperative work!")

        si: ScheduledItem[typing.TState] = ScheduledItem(self, state, action, duetime)

        with self.lock:
            self.queue.enqueue(si)

            if self.idle:
                self.idle = False
                start_trampoline = True
            else:
                start_trampoline = False

        if start_trampoline:
            while True:
                try:
                    TrampolineScheduler.Trampoline.run(self.queue)
                finally:
                    with self.lock:
                        if not self.queue:
                            self.idle = True
                            # self.queue.clear()
                            break

        return si.disposable
