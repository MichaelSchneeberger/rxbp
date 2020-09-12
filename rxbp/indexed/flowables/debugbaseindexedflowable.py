from dataclasses import dataclass, replace
from traceback import FrameSummary
from typing import List

from rxbp.indexed.indexedsubscription import IndexedSubscription
from rxbp.indexed.mixins.indexedflowablemixin import IndexedFlowableMixin
from rxbp.indexed.selectors.flowablebase import FlowableBase
from rxbp.indexed.selectors.flowablebaseandselectors import FlowableBaseAndSelectors
from rxbp.observables.init.initdebugobservable import init_debug_observable
from rxbp.subscriber import Subscriber


@dataclass
class DebugBaseIndexedFlowable(IndexedFlowableMixin):
    source: IndexedFlowableMixin
    base: FlowableBase
    name: str
    stack: List[FrameSummary]

    def unsafe_subscribe(self, subscriber: Subscriber) -> IndexedSubscription:
        subscription = self.source.unsafe_subscribe(subscriber=subscriber)

        if self.base in subscription.index.selectors:
            selector = init_debug_observable(
                source=subscription.index.selectors[self.base],
                name=self.name,
                subscriber=subscriber,
                stack=self.stack,
            )

            subscription = subscription.copy(index=replace(
                subscription.index,
                selectors={**subscription.index.selectors, **{self.base: selector}}
            ))

        return subscription
