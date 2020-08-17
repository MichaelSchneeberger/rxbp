from rxbp.mixins.flowablebasemixin import FlowableBaseMixin
from rxbp.observables.tolistobservable import ToListObservable
from rxbp.selectors.bases.numericalbase import NumericalBase
from rxbp.selectors.baseandselectors import BaseAndSelectors
from rxbp.subscriber import Subscriber
from rxbp.subscription import Subscription


class ToListFlowable(FlowableBaseMixin):
    def __init__(self, source: FlowableBaseMixin):
        super().__init__()

        self._source = source

    def unsafe_subscribe(self, subscriber: Subscriber) -> Subscription:
        subscription = self._source.unsafe_subscribe(subscriber=subscriber)
        observable = ToListObservable(source=subscription.observable)

        # to_list emits exactly one element
        base = NumericalBase(1)

        return init_subscription(BaseAndSelectors(base=base), observable=observable)