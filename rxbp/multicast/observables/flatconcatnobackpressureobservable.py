from dataclasses import dataclass
from typing import Callable, Any

from rx.disposable import CompositeDisposable

from rxbp.multicast.observer.flatconcatnobackpressureobserver import FlatConcatNoBackpressureObserver
from rxbp.observable import Observable
from rxbp.observerinfo import ObserverInfo
from rxbp.scheduler import Scheduler


@dataclass
class FlatConcatNoBackpressureObservable(Observable):
    source: Observable
    selector: Callable[[Any], Observable]
    scheduler: Scheduler
    subscribe_scheduler: Scheduler

    def observe(self, observer_info: ObserverInfo):
        composite_disposable = CompositeDisposable()

        concat_observer = FlatConcatNoBackpressureObserver(
            next_observer=observer_info.observer,
            selector=self.selector,
            scheduler=self.scheduler,
            subscribe_scheduler=self.subscribe_scheduler,
            composite_disposable=composite_disposable,
        )

        disposable = self.source.observe(observer_info.copy(observer=concat_observer))
        composite_disposable.add(disposable)

        return composite_disposable
