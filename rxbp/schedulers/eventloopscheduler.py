from rx.concurrency import EventLoopScheduler as ParentEventLoopScheduler

from rxbp.scheduler import SchedulerBase


class EventLoopScheduler(SchedulerBase, ParentEventLoopScheduler):
    pass
