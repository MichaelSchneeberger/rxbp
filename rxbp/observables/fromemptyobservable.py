from dataclasses import dataclass

import rx

from rxbp.observable import Observable
from rxbp.observerinfo import ObserverInfo
from rxbp.scheduler import Scheduler


@dataclass
class FromEmptyObservable(Observable):
    subscribe_scheduler: Scheduler

    def observe(self, observer_info: ObserverInfo) -> rx.typing.Disposable:
        def action(_, __):
            observer_info.observer.on_completed()

        return self.subscribe_scheduler.schedule(action)
