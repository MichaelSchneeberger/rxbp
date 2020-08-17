from dataclasses import dataclass
from typing import Callable, Any

from rxbp.indexed.init.initindexedsubscription import init_indexed_subscription
from rxbp.indexed.mixins.indexedflowablemixin import IndexedFlowableMixin
from rxbp.indexed.observables.indexedfilterobservable import IndexedFilterObservable
from rxbp.mixins.flowablebasemixin import FlowableBaseMixin
from rxbp.observables.filterobservable import FilterObservable
from rxbp.observablesubjects.publishosubject import PublishOSubject
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

        selector_subject = PublishOSubject(scheduler=subscriber.scheduler)

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
