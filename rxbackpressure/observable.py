from rx.concurrency.schedulerbase import SchedulerBase

from rxbackpressure.observer import Observer
from rxbackpressure.scheduler import SchedulerBase


class Observable:
    def unsafe_subscribe(self, observer: Observer, scheduler: SchedulerBase,
                         subscribe_scheduler: SchedulerBase):
        raise NotImplementedError

    def subscribe(self, observer: Observer, scheduler: SchedulerBase,
                  subscribe_scheduler: SchedulerBase):
        def action(_, __):
            self.unsafe_subscribe(observer, scheduler, subscribe_scheduler)

        subscribe_scheduler.schedule(action)
