from dataclasses import dataclass
from typing import Callable, Any

from rxbp.observable import Observable
from rxbp.observerinfo import ObserverInfo
from rxbp.observers.filterobserver import FilterObserver


@dataclass
class FilterObservable(Observable):
    source: Observable
    predicate: Callable[[Any], bool]

    def observe(self, observer_info: ObserverInfo):
        subscription = observer_info.copy(
            observer=FilterObserver(
                observer=observer_info.observer,
                predicate=self.predicate,
            ),
        )
        return self.source.observe(subscription)
