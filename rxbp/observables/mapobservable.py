from dataclasses import dataclass
from typing import Callable, Any

from rxbp.observable import Observable
from rxbp.observerinfo import ObserverInfo
from rxbp.observers.mapobserver import MapObserver


@dataclass
class MapObservable(Observable):
    source: Observable
    func: Callable[[Any], Any]
    # stack: List[FrameSummary]

    def observe(self, observer_info: ObserverInfo):
        return self.source.observe(observer_info.copy(
            observer=MapObserver(
                source=observer_info.observer,
                func=self.func,
            ),
        ))

        # except Exception:
        #     raise Exception(to_operator_exception(
        #         message=f'something went wrong when observing {self.source}',
        #         stack=self.stack,
        #     ))
