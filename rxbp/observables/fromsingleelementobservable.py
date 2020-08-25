from dataclasses import dataclass
from typing import Callable

import rx

from rxbp.observable import Observable
from rxbp.observerinfo import ObserverInfo
from rxbp.scheduler import Scheduler
from rxbp.typing import ElementType


@dataclass
class FromSingleElementObservable(Observable):
    lazy_elem: Callable[[], ElementType]
    subscribe_scheduler: Scheduler

    def observe(self, observer_info: ObserverInfo) -> rx.typing.Disposable:
        observer = observer_info.observer

        def action(_, __):
            _ = observer.on_next(self.lazy_elem())
            observer.on_completed()

        return self.subscribe_scheduler.schedule(action)
