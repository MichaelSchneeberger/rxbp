from dataclasses import dataclass
from typing import Callable, Any

from rxbp.observable import Observable
from rxbp.observerinfo import ObserverInfo
from rxbp.observers.defaultifemptyobserver import DefaultIfEmptyObserver


@dataclass
class DefaultIfEmptyObservable(Observable):
    source: Observable
    lazy_val: Callable[[], Any]

    def observe(self, observer_info: ObserverInfo):
        first_observer = DefaultIfEmptyObserver(
            next_observer=observer_info.observer,
            lazy_val=self.lazy_val,
        )
        map_subscription = observer_info.copy(observer=first_observer)
        return self.source.observe(map_subscription)
