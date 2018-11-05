from rx.concurrency.schedulerbase import SchedulerBase

from rxbackpressure.ack import Continue, Stop
from rxbackpressure.observers.anonymousobserver import AnonymousObserver
from rxbackpressure.observable import Observable
from rxbackpressure.observer import Observer


class DebugObservable(Observable):
    def __init__(self, source, name, on_next=None, on_completed=None, on_ack=None, on_subscribe=None, print_ack=False):
        self.source = source
        self.print_ack = print_ack
        self.name = name

        self.on_next_func = on_next or (lambda v: print('{}.on_next {}'.format(name, v)))
        self.on_completed_func = on_completed or (lambda: print('{}.on_completed'.format(name)))
        self.on_subscribe_func = on_subscribe or (lambda v: print('{}.on_subscribe {}'.format(name, v)))
        self.on_ack_func = on_ack or (lambda v: print('{}.on_ack {}'.format(name, v)))

    def unsafe_subscribe(self, observer: Observer, scheduler: SchedulerBase,
                         subscribe_scheduler: SchedulerBase):
        self.on_subscribe_func(observer)

        def on_next(v):
            self.on_next_func(v)
            ack = observer.on_next(v)

            if isinstance(ack, Continue) or isinstance(ack, Stop):
                self.on_ack_func(ack)
            else:
                if self.print_ack:
                    print('{}.on_raw_ack {}'.format(self.name, ack))
                ack.subscribe(self.on_ack_func)
            return ack

        def on_completed():
            self.on_completed_func()
            return observer.on_completed()

        map_observer = AnonymousObserver(on_next=on_next, on_error=observer.on_error,
                                     on_completed=on_completed)
        self.source.unsafe_subscribe(map_observer, scheduler, subscribe_scheduler)