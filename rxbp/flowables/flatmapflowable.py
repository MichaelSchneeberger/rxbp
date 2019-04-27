from typing import Any, Callable

from rxbp.flowablebase import FlowableBase
from rxbp.observables.flatmapobservable import FlatMapObservable
from rxbp.subscriber import Subscriber


class FlatMapFlowable(FlowableBase):
    def __init__(self, source: FlowableBase, selector: Callable[[Any], FlowableBase]):
        super().__init__()

        self._source = source
        self._selector = selector

    def unsafe_subscribe(self, subscriber: Subscriber) -> FlowableBase.FlowableReturnType:
        source_observable, source_selectors = self._source.unsafe_subscribe(subscriber=subscriber)

        def observable_selector(elem: Any):
            flowable = self._selector(elem)
            inner_obs, _ = flowable.unsafe_subscribe(subscriber=subscriber)
            return inner_obs

        obs = FlatMapObservable(source=source_observable, selector=observable_selector,
                                scheduler=subscriber.scheduler)
        return obs, source_selectors