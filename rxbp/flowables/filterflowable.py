from typing import Callable, Any

from rxbp.flowablebase import FlowableBase
from rxbp.observables.filterobservable import FilterObservable
from rxbp.selectors.baseandselectors import BaseAndSelectors
from rxbp.selectors.selectionop import merge_selectors
from rxbp.subscriber import Subscriber
from rxbp.subscription import Subscription


class FilterFlowable(FlowableBase):
    def __init__(
            self,
            source: FlowableBase,
            predicate: Callable[[Any], bool],
    ):
        super().__init__()

        self._source = source
        self._predicate = predicate

    def unsafe_subscribe(self, subscriber: Subscriber) -> Subscription:
        subscription = self._source.unsafe_subscribe(subscriber)

        observable = FilterObservable(
            source=subscription.observable,
            predicate=self._predicate,
            scheduler=subscriber.scheduler,
        )

        # apply filter selector to each selector
        def gen_selectors():
            if subscription.info.selectors is not None:
                for base, indexing in subscription.info.selectors.items():
                    yield base, merge_selectors(indexing, observable.selector,
                                                scheduler=subscriber.scheduler)

            if subscription.info.base is not None:
                yield subscription.info.base, observable.selector

        selectors = dict(gen_selectors())

        return Subscription(info=BaseAndSelectors(base=None, selectors=selectors), observable=observable)