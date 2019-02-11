from rxbackpressurebatched.ack import Continue, Stop, stop_ack
from rxbackpressurebatched.observable import Observable
from rxbackpressurebatched.observer import Observer
from rxbackpressurebatched.scheduler import SchedulerBase, ExecutionModel


class RepeatFirstObservable(Observable):
    def __init__(self, source):
        self.source = source

    def unsafe_subscribe(self, observer: Observer, scheduler: SchedulerBase,
                         subscribe_scheduler: SchedulerBase):

        class RepeatFirstObserver(Observer):
            def on_next(self, v):

                def action(_, __):
                    while True:
                        ack = observer.on_next(v)

                        if isinstance(ack, Continue):
                            pass
                        elif isinstance(ack, Stop):
                            break
                        else:
                            def _(ack):
                                if isinstance(ack, Continue):
                                    scheduler.schedule(action)

                            ack.subscribe(_)
                            break

                scheduler.schedule(action)
                return stop_ack

            def on_error(self, exc):
                return observer.on_error(exc)

            def on_completed(self):
                return observer.on_completed()

        map_observer = RepeatFirstObserver()
        self.source.unsafe_subscribe(map_observer, scheduler, subscribe_scheduler)
