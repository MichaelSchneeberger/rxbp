from typing import Any, Callable

from rxbp.flowablebase import FlowableBase
from rxbp.observables.flatmapobservable import FlatMapObservable
from rxbp.selectors.bases import Base
from rxbp.subscriber import Subscriber
from rxbp.subscription import Subscription


class FlatMapFlowable(FlowableBase):
    def __init__(self, source: FlowableBase, selector: Callable[[Any], Base]):
        # base becomes undefined after flat mapping
        base = None

        super().__init__(base=base)

        self._source = source
        self._selector = selector

    def unsafe_subscribe(self, subscriber: Subscriber) -> Subscription:
        source_observable, source_selectors = self._source.unsafe_subscribe(subscriber=subscriber)

        def observable_selector(elem: Any):
            flowable = self._selector(elem)
            inner_obs, _ = flowable.unsafe_subscribe(subscriber=subscriber)
            return inner_obs

        obs = FlatMapObservable(source=source_observable, selector=observable_selector,
                                scheduler=subscriber.scheduler, subscribe_scheduler=subscriber.subscribe_scheduler)
        return obs, source_selectors