from rx.concurrency import CurrentThreadScheduler as ParentCurrentThreadScheduler, \
    current_thread_scheduler as parent_current_thread_scheduler

from rxbackpressurebatched.scheduler import SchedulerBase


class CurrentThreadScheduler(SchedulerBase, ParentCurrentThreadScheduler):
    pass


class CurrentThreadSchedulerAdapter(SchedulerBase):
    @property
    def now(self):
        return parent_current_thread_scheduler.now

    def schedule(self, action, state=None):
        return parent_current_thread_scheduler.schedule(action, state)

    def schedule_relative(self, duetime, action, state=None):
        return parent_current_thread_scheduler.schedule_relative(duetime, action, state)

    def schedule_absolute(self, duetime, action, state=None):
        return parent_current_thread_scheduler.schedule_absolute(duetime, action, state)


current_thread_scheduler = CurrentThreadSchedulerAdapter()