from typing import Callable, Any

from rxbp.flowablebase import FlowableBase
from rxbp.observables.mapobservable import MapObservable
from rxbp.subscriber import Subscriber
from rxbp.typing import ValueType


class MapFlowable(FlowableBase):
    def __init__(self, source: FlowableBase, selector: Callable[[ValueType], Any]):
        super().__init__(base=source.base, selectable_bases=source.selectable_bases)

        self._source = source
        self._selector = selector

    def unsafe_subscribe(self, subscriber: Subscriber) -> FlowableBase.FlowableReturnType:
        source_observable, source_selectors = self._source.unsafe_subscribe(subscriber=subscriber)
        obs = MapObservable(source=source_observable, selector=self._selector)
        return obs, source_selectors
