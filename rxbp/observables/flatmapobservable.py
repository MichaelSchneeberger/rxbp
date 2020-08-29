import threading
from dataclasses import dataclass
from typing import Callable, Any

from rx.disposable import CompositeDisposable

from rxbp.observable import Observable
from rxbp.observerinfo import ObserverInfo
from rxbp.observers.flatmapobserver import FlatMapObserver
from rxbp.scheduler import Scheduler
from rxbp.states.rawstates.rawflatmapstates import RawFlatMapStates


@dataclass
class FlatMapObservable(Observable):
    source: Observable
    func: Callable[[Any], Observable]
    scheduler: Scheduler
    subscribe_scheduler: Scheduler
    delay_errors: bool = False

    def observe(self, observer_info: ObserverInfo):
        composite_disposable = CompositeDisposable()

        disposable = self.source.observe(
            observer_info.copy(observer=FlatMapObserver(
                observer_info=observer_info,
                func=self.func,
                scheduler=self.scheduler,
                subscribe_scheduler=self.subscribe_scheduler,
                composite_disposable=composite_disposable,
            )),
        )
        composite_disposable.add(disposable)

        return composite_disposable
