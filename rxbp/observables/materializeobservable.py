from dataclasses import dataclass
from typing import Callable, Any

from rxbp.acknowledgement.ack import Ack
from rxbp.observable import Observable
from rxbp.observer import Observer
from rxbp.observerinfo import ObserverInfo
from rxbp.observers.mapobserver import MapObserver
from rxbp.typing import ElementType


@dataclass
class MaterializeObservable(Observable):
    source: Observable

    def observe(self, observer_info: ObserverInfo):
        @dataclass
        class MaterializeObserver(Observer):
            source: Observer

            def on_next(self, elem: ElementType) -> Ack:
                if not isinstance(elem, list):
                    elem = list(elem)

                return self.source.on_next(elem)

            def on_error(self, exc: Exception):
                return self.source.on_error(exc)

            def on_completed(self):
                return self.source.on_completed()

        return self.source.observe(observer_info.copy(
            observer=MaterializeObserver(
                source=observer_info.observer,
            ),
        ))
