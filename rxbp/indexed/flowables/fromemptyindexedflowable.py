from rxbp.indexed.init.initindexedsubscription import init_indexed_subscription
from rxbp.indexed.mixins.indexedflowablemixin import IndexedFlowableMixin
from rxbp.indexed.selectors.bases.numericalbase import NumericalBase
from rxbp.indexed.selectors.flowablebaseandselectors import FlowableBaseAndSelectors
from rxbp.observables.fromemptyobservable import FromEmptyObservable
from rxbp.subscriber import Subscriber
from rxbp.subscription import Subscription


class FromEmptyIndexedFlowable(IndexedFlowableMixin):
    def unsafe_subscribe(self, subscriber: Subscriber) -> Subscription:
        return init_indexed_subscription(
            index=FlowableBaseAndSelectors(
                base=NumericalBase(0),
            ),
            observable=FromEmptyObservable(
                subscribe_scheduler=subscriber.subscribe_scheduler,
            ),
        )
