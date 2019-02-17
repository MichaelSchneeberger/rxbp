from typing import Callable, Any

from rxbp.ack import Continue, continue_ack
from rxbp.observer import Observer
from rxbp.observers.anonymousobserver import AnonymousObserver
from rxbp.scheduler import Scheduler
from rxbp.schedulers.currentthreadscheduler import CurrentThreadScheduler


class Observable:
    def unsafe_subscribe(self, observer: Observer, scheduler: Scheduler,
                         subscribe_scheduler: Scheduler):
        raise NotImplementedError

    def subscribe(self, observer: Observer, scheduler: Scheduler = None,
                  subscribe_scheduler: Scheduler = None):
        subscribe_scheduler_ = subscribe_scheduler or CurrentThreadScheduler()
        scheduler_ = scheduler or subscribe_scheduler_

        def action(_, __):
            return self.unsafe_subscribe(observer, scheduler_, subscribe_scheduler_)

        return subscribe_scheduler_.schedule(action)

    def subscribe_with(self,
                       on_next: Callable[[Any], None] = None,
                       on_error: Callable[[Any], None] = None,
                       on_completed: Callable[[], None] = None,
                       scheduler: Scheduler = None,
                       subscribe_scheduler: Scheduler = None):
        def on_next_with_ack(v):
            on_next(v)
            return continue_ack

        on_next_ = (lambda v: continue_ack) if on_next is None else on_next_with_ack
        on_error_ = on_error or (lambda err: None)
        on_completed_ = on_completed or (lambda: None)

        observer = AnonymousObserver(on_next=on_next_, on_error=on_error_, on_completed=on_completed_)

        return self.subscribe(observer=observer, scheduler=scheduler, subscribe_scheduler=subscribe_scheduler)
