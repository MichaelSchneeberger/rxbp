from dataclasses import dataclass
from traceback import FrameSummary
from typing import Callable, Any, List

from rxbp.indexed.indexedsubscription import IndexedSubscription
from rxbp.indexed.mixins.indexedflowablemixin import IndexedFlowableMixin
from rxbp.indexed.observables.filterindexedobservable import FilterIndexedObservable
from rxbp.indexed.selectors.flowablebaseandselectors import FlowableBaseAndSelectors
from rxbp.indexed.selectors.selectionop import merge_selectors
from rxbp.observablesubjects.publishobservablesubject import PublishObservableSubject
from rxbp.subscriber import Subscriber


@dataclass(frozen=True)
class FilterIndexedFlowable(IndexedFlowableMixin):
    source: IndexedFlowableMixin
    predicate: Callable[[Any], bool]
    stack: List[FrameSummary]

    def unsafe_subscribe(self, subscriber: Subscriber) -> IndexedSubscription:
        subscription = self.source.unsafe_subscribe(subscriber)

        observable = FilterIndexedObservable(
            source=subscription.observable,
            predicate=self.predicate,
            selector_subject=PublishObservableSubject(),
        )

        # apply filter selector to each selector
        def gen_selectors():
            if subscription.index.selectors is not None:
                for base, indexing in subscription.index.selectors.items():
                    yield base, merge_selectors(
                        left=indexing,
                        right=observable.selector_subject,
                        subscribe_scheduler=subscriber.scheduler,
                        stack=self.stack,
                    )

            if subscription.index.base is not None:
                yield subscription.index.base, observable.selector_subject

        selectors = dict(gen_selectors())

        return subscription.copy(
            index=FlowableBaseAndSelectors(base=None, selectors=selectors),
            observable=observable,
        )
