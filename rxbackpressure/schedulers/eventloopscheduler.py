from rx.concurrency import EventLoopScheduler as ParentEventLoopScheduler

from rxbackpressure.scheduler import SchedulerBase


class EventLoopScheduler(SchedulerBase, ParentEventLoopScheduler):
    pass
