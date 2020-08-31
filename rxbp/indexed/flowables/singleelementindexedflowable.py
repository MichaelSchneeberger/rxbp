from dataclasses import dataclass
from typing import Callable

from rxbp.indexed.indexedsubscription import IndexedSubscription
from rxbp.indexed.init.initindexedsubscription import init_indexed_subscription
from rxbp.indexed.mixins.indexedflowablemixin import IndexedFlowableMixin
from rxbp.observables.fromsingleelementobservable import FromSingleElementObservable
from rxbp.indexed.selectors.flowablebase import FlowableBase
from rxbp.indexed.selectors.flowablebaseandselectors import FlowableBaseAndSelectors
from rxbp.subscriber import Subscriber
from rxbp.typing import ElementType


@dataclass
class SingleElementIndexedFlowable(IndexedFlowableMixin):
    lazy_elem: Callable[[], ElementType]
    base: FlowableBase

    def unsafe_subscribe(self, subscriber: Subscriber) -> IndexedSubscription:
        return init_indexed_subscription(
            observable=FromSingleElementObservable(
                lazy_elem=self.lazy_elem,
                subscribe_scheduler=subscriber.subscribe_scheduler,
            ),
            index=FlowableBaseAndSelectors(
                base=self.base,
            ),
        )

