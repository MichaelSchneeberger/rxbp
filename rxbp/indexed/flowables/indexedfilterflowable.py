from dataclasses import dataclass
from typing import Callable, Any

from rxbp.indexed.mixins.indexedflowablemixin import IndexedFlowableMixin
from rxbp.indexed.observables.indexedfilterobservable import IndexedFilterObservable
from rxbp.observablesubjects.publishobservablesubject import PublishObservableSubject
from rxbp.selectors.baseandselectors import BaseAndSelectors
from rxbp.selectors.selectionop import merge_selectors
from rxbp.subscriber import Subscriber
from rxbp.subscription import Subscription


@dataclass
class IndexedFilterFlowable(IndexedFlowableMixin):
    source: IndexedFlowableMixin
    predicate: Callable[[Any], bool]

    def unsafe_subscribe(self, subscriber: Subscriber) -> Subscription:
        subscription = self.source.unsafe_subscribe(subscriber)

        selector_subject = PublishObservableSubject(scheduler=subscriber.scheduler)

        observable = IndexedFilterObservable(
            source=subscription.observable,
            predicate=self.predicate,
            selector_subject=selector_subject,
        )

        # apply filter selector to each selector
        def gen_selectors():
            if subscription.index.selectors is not None:
                for base, indexing in subscription.index.selectors.items():
                    yield base, merge_selectors(indexing, observable.selector_subject,
                                                scheduler=subscriber.scheduler)

            if subscription.index.base is not None:
                yield subscription.index.base, observable.selector_subject

        selectors = dict(gen_selectors())

        return subscription.copy(
            index=BaseAndSelectors(base=None, selectors=selectors),
            observable=observable,
        )
