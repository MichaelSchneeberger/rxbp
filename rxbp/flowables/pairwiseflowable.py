from rxbp.flowablebase import FlowableBase
from rxbp.observables.pairwiseobservable import PairwiseObservable
from rxbp.selectors.base import Base
from rxbp.selectors.bases.pairwisebase import PairwiseBase
from rxbp.selectors.baseandselectors import BaseAndSelectors
from rxbp.subscriber import Subscriber
from rxbp.subscription import Subscription


class PairwiseFlowable(FlowableBase):
    def __init__(self, source: FlowableBase):
        super().__init__()

        self._source = source

    def unsafe_subscribe(self, subscriber: Subscriber) -> Subscription:
        subscription = self._source.unsafe_subscribe(subscriber=subscriber)

        if isinstance(subscription.info.base, Base):
            base = PairwiseBase(subscription.info.base)
        else:
            base = None

        observable = PairwiseObservable(source=subscription.observable)

        return Subscription(
            BaseAndSelectors(base=base, selectors=subscription.info.selectors),
            observable=observable,
        )