from rxbp.observable import Observable
from rxbp.observerinfo import ObserverInfo
from rxbp.observers.observeonobserver import ObserveOnObserver
from rxbp.scheduler import Scheduler


class ObserveOnObservable(Observable):
    def __init__(
            self,
            source: Observable,
            scheduler: Scheduler,
    ):
        self.source = source
        self.scheduler = scheduler

    def observe(self, observer_info: ObserverInfo):
        return self.source.observe(observer_info.copy(
            observer=ObserveOnObserver(
                observer=observer_info.observer,
                scheduler=self.scheduler,
            )
        ))
