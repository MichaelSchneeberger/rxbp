from rxbp.ack.ackimpl import Continue, Stop
from rxbp.ack.acksubject import AckSubject
from rxbp.observable import Observable
from rxbp.observer import Observer
from rxbp.observerinfo import ObserverInfo
from rxbp.scheduler import Scheduler
from rxbp.typing import ElementType


class ObserveOnObservable(Observable):
    def __init__(self, source: Observable, scheduler: Scheduler):
        self.source = source
        self.scheduler = scheduler

    def observe(self, observer_info: ObserverInfo):
        observer = observer_info.observer

        class ObserveOnObserver(Observer):
            def __init__(self, scheduler: Scheduler):
                self.scheduler = scheduler

            def on_next(self, elem: ElementType):
                ack_subject = AckSubject()

                def action(_, __):
                    inner_ack = observer.on_next(elem)

                    if isinstance(inner_ack, Continue):
                        ack_subject.on_next(inner_ack)
                    elif isinstance(inner_ack, Stop):
                        ack_subject.on_next(inner_ack)
                    else:
                        inner_ack.subscribe(ack_subject)

                self.scheduler.schedule(action)
                return ack_subject

            def on_error(self, exc):
                def action(_, __):
                    observer.on_error(exc)

                self.scheduler.schedule(action)

            def on_completed(self):
                def action(_, __):
                    observer.on_completed()

                self.scheduler.schedule(action)

        observe_on_observer = ObserveOnObserver(scheduler=self.scheduler)
        observer_on_subscription = ObserverInfo(observe_on_observer, is_volatile=observer_info.is_volatile)
        return self.source.observe(observer_on_subscription)
