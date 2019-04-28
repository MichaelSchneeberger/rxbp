from typing import Callable, Any

from rxbp.flowablebase import FlowableBase
from rxbp.selectors.selectionop import merge_selectors
from rxbp.observables.filterobservable import FilterObservable
from rxbp.subscriber import Subscriber


class FilterFlowable(FlowableBase):
    def __init__(self, source: FlowableBase, predicate: Callable[[Any], bool]):
        if source.base is not None:
            selectable_bases = source.selectable_bases | {source.base}
        else:
            selectable_bases = source.selectable_bases

        # base becomes undefined after filtering
        super().__init__(base=None, selectable_bases=selectable_bases)

        self._source = source
        self._predicate = predicate

    def unsafe_subscribe(self, subscriber: Subscriber) -> FlowableBase.FlowableReturnType:
        source_observable, source_selectors = self._source.unsafe_subscribe(subscriber)

        observable = FilterObservable(source=source_observable, predicate=self._predicate, scheduler=subscriber.scheduler)

        # apply filter selector to each selector
        def gen_merged_selector():
            for base, indexing in source_selectors.items():
                yield base, merge_selectors(indexing, observable.selector,
                                            scheduler=subscriber.scheduler)

        selectors = dict(gen_merged_selector())

        if self._source.base is not None:
            selectors_ = {**selectors, **{self._source.base: observable.selector}}
        else:
            selectors_ = selectors

        return observable, selectors_