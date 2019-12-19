from typing import Callable, Any

from rxbp.multicast.observer.flatconcatnobackpressureobserver import FlatConcatNoBackpressureObserver
from rxbp.observable import Observable
from rxbp.observerinfo import ObserverInfo
from rxbp.scheduler import Scheduler


class FlatConcatNoBackpressureObservable(Observable):
    def __init__(
            self,
            source: Observable,
            selector: Callable[[Any], Observable],
            scheduler: Scheduler,
            subscribe_scheduler: Scheduler,
    ):
        super().__init__()

        self.source = source
        self.selector = selector
        self.scheduler = scheduler
        self.subscribe_scheduler = subscribe_scheduler

    def observe(self, observer_info: ObserverInfo):
        observer = observer_info.observer
        scheduler = self.scheduler
        subscribe_scheduler = self.subscribe_scheduler

        concat_observer = FlatConcatNoBackpressureObserver(
            observer=observer,
            selector=self.selector,
            scheduler=scheduler,
            subscribe_scheduler=subscribe_scheduler,
            is_volatile=observer_info.is_volatile,
        )
        disposable = self.source.observe(observer_info.copy(observer=concat_observer))
        return disposable
