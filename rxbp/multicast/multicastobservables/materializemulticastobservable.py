from dataclasses import dataclass
from typing import Callable, Any

from rxbp.acknowledgement.ack import Ack
from rxbp.multicast.multicastobservable import MultiCastObservable
from rxbp.multicast.multicastobserver import MultiCastObserver
from rxbp.multicast.multicastobserverinfo import MultiCastObserverInfo
from rxbp.observable import Observable
from rxbp.observer import Observer
from rxbp.observerinfo import ObserverInfo
from rxbp.observers.mapobserver import MapObserver
from rxbp.typing import ElementType


@dataclass
class MaterializeMultiCastObservable(MultiCastObservable):
    source: MultiCastObservable

    def observe(self, observer_info: MultiCastObserverInfo):
        @dataclass
        class MaterializeMultiCastObserver(MultiCastObserver):
            source: MultiCastObserver

            def on_next(self, item: ElementType):
                if not isinstance(item, list):
                    item = list(item)

                self.source.on_next(item)

            def on_error(self, exc: Exception):
                return self.source.on_error(exc)

            def on_completed(self):
                return self.source.on_completed()

        return self.source.observe(observer_info.copy(
            observer=MaterializeMultiCastObserver(
                source=observer_info.observer,
            ),
        ))
