from rx.concurrency.schedulerbase import SchedulerBase

from rxbp.ack import Continue, Stop, Ack
from rxbp.observers.anonymousobserver import AnonymousObserver
from rxbp.observable import Observable
from rxbp.observer import Observer
from rxbp.scheduler import Scheduler


class ObserveOnObservable(Observable):
    def __init__(self, source: Observable, scheduler: Scheduler):
        self.source = source
        self.scheduler = scheduler

    def observe(self, observer: Observer):
        def on_next(v):
            def action(_, __):
                inner_ack = observer.on_next(v)

                if isinstance(inner_ack, Continue):
                    ack.on_next(inner_ack)
                    ack.on_completed()
                elif isinstance(inner_ack, Stop):
                    ack.on_next(inner_ack)
                    ack.on_completed()
                else:
                    inner_ack.subscribe(ack)

            self.scheduler.schedule(action)

            ack = Ack()
            return ack

        def on_error(exc):
            def action(_, __):
                observer.on_error(exc)

            self.scheduler.schedule(action)

        def on_completed():
            def action(_, __):
                observer.on_completed()

            self.scheduler.schedule(action)

        observe_on_observer = AnonymousObserver(on_next_func=on_next, on_error_func=on_error,
                                                on_completed_func=on_completed)
        return self.source.observe(observe_on_observer)
