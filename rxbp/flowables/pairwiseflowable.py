from rxbp.init.initsubscription import init_subscription
from rxbp.mixins.flowablemixin import FlowableMixin
from rxbp.observables.pairwiseobservable import PairwiseObservable
from rxbp.selectors.base import Base
from rxbp.selectors.bases.pairwisebase import PairwiseBase
from rxbp.selectors.baseandselectors import BaseAndSelectors
from rxbp.subscriber import Subscriber
from rxbp.subscription import Subscription


class PairwiseFlowable(FlowableMixin):
    def __init__(self, source: FlowableMixin):
        super().__init__()

        self._source = source

    def unsafe_subscribe(self, subscriber: Subscriber) -> Subscription:
        subscription = self._source.unsafe_subscribe(subscriber=subscriber)
        observable = PairwiseObservable(source=subscription.observable)
        return init_subscription(observable=observable)
