from dataclasses import dataclass
from typing import Callable, Any

from rxbp.observable import Observable
from rxbp.observerinfo import ObserverInfo
from rxbp.observers.mapobserver import MapObserver


@dataclass
class MapObservable(Observable):
    source: Observable
    func: Callable[[Any], Any]

    def observe(self, observer_info: ObserverInfo):
        observer = MapObserver(
            source=observer_info.observer,
            func=self.func,
        )

        subscription = observer_info.copy(
            observer=observer,
        )

        return self.source.observe(subscription)
