from dataclasses import dataclass

from rxbp.indexed.indexedsubscription import IndexedSubscription
from rxbp.indexed.mixins.indexedflowablemixin import IndexedFlowableMixin
from rxbp.indexed.selectors.bases.pairwisebase import PairwiseBase
from rxbp.indexed.selectors.flowablebase import FlowableBase
from rxbp.indexed.selectors.flowablebaseandselectors import FlowableBaseAndSelectors
from rxbp.observables.pairwiseobservable import PairwiseObservable
from rxbp.subscriber import Subscriber


@dataclass
class PairwiseIndexedFlowable(IndexedFlowableMixin):
    source: IndexedFlowableMixin

    def unsafe_subscribe(self, subscriber: Subscriber) -> IndexedSubscription:
        subscription = self.source.unsafe_subscribe(subscriber=subscriber)

        if isinstance(subscription.index.base, FlowableBase):
            base = PairwiseBase(subscription.index.base)
        else:
            base = None

        observable = PairwiseObservable(source=subscription.observable)

        return subscription.copy(
            index=FlowableBaseAndSelectors(base=base, selectors=subscription.index.selectors),
            observable=observable,
        )