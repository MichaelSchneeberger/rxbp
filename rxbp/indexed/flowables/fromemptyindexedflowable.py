from rxbp.indexed.init.initindexedsubscription import init_indexed_subscription
from rxbp.mixins.flowablebasemixin import FlowableBaseMixin
from rxbp.observables.fromemptyobservable import FromEmptyObservable
from rxbp.selectors.baseandselectors import BaseAndSelectors
from rxbp.selectors.bases.numericalbase import NumericalBase
from rxbp.subscriber import Subscriber
from rxbp.subscription import Subscription


class FromEmptyIndexedFlowable(FlowableBaseMixin):
    def unsafe_subscribe(self, subscriber: Subscriber) -> Subscription:
        return init_indexed_subscription(
            index=BaseAndSelectors(
                base=NumericalBase(0),
            ),
            observable=FromEmptyObservable(
                subscribe_scheduler=subscriber.subscribe_scheduler,
            ),
        )
