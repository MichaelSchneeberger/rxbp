from rxbp.flowablebase import FlowableBase
from rxbp.observables.mergeobservable import MergeObservable
from rxbp.selectors.baseandselectors import BaseAndSelectors
from rxbp.subscriber import Subscriber
from rxbp.subscription import Subscription


class MergeFlowable(FlowableBase):
    def __init__(self, source: FlowableBase, other: FlowableBase):
        super().__init__()

        self._source = source
        self._other = other

    def unsafe_subscribe(self, subscriber: Subscriber):
        left_subscription = self._source.unsafe_subscribe(subscriber=subscriber)
        right_subscription = self._other.unsafe_subscribe(subscriber=subscriber)
        observable = MergeObservable(left=left_subscription.observable, right=right_subscription.observable)

        # the base becomes anonymous after merging
        base = None

        info = BaseAndSelectors(
            base=base,
        )

        return Subscription(
            info=info,
            observable=observable,
        )
