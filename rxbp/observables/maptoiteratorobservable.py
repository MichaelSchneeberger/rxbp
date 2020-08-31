from dataclasses import dataclass
from typing import Callable, Any, Iterator

from rxbp.observable import Observable
from rxbp.observerinfo import ObserverInfo
from rxbp.observers.maptoiteratorobserver import MapToIteratorObserver


@dataclass(frozen=True)
class MapToIteratorObservable(Observable):
    source: Observable
    func: Callable[[Any], Iterator[Any]]

    def observe(self, observer_info: ObserverInfo):
        func = self.func

        map_subscription = observer_info.copy(
            observer=MapToIteratorObserver(
                source=observer_info.observer,
                func=func,
            ),
        )
        return self.source.observe(map_subscription)
