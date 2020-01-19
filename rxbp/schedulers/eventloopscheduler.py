from rx.scheduler.eventloopscheduler import EventLoopScheduler as ParentEventLoopScheduler

from rxbp.scheduler import SchedulerBase


class EventLoopScheduler(SchedulerBase, ParentEventLoopScheduler):
    @property
    def is_order_guaranteed(self) -> bool:
        return True
