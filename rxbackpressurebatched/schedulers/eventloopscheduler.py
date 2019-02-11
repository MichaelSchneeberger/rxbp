from rx.concurrency import EventLoopScheduler as ParentEventLoopScheduler

from rxbackpressurebatched.scheduler import SchedulerBase


class EventLoopScheduler(SchedulerBase, ParentEventLoopScheduler):
    pass
