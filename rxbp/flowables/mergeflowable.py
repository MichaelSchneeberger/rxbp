from rxbp.init.initsubscription import init_subscription
from rxbp.mixins.flowablemixin import FlowableMixin
from rxbp.observables.mergeobservable import MergeObservable
from rxbp.selectors.baseandselectors import BaseAndSelectors
from rxbp.subscriber import Subscriber
from rxbp.subscription import Subscription


class MergeFlowable(FlowableMixin):
    def __init__(self, source: FlowableMixin, other: FlowableMixin):
        super().__init__()

        self._source = source
        self._other = other

    def unsafe_subscribe(self, subscriber: Subscriber):
        left_subscription = self._source.unsafe_subscribe(subscriber=subscriber)
        right_subscription = self._other.unsafe_subscribe(subscriber=subscriber)
        observable = MergeObservable(left=left_subscription.observable, right=right_subscription.observable)

        # # the base becomes anonymous after merging
        # base = None

        # info = BaseAndSelectors(
        #     base=base,
        # )

        return init_subscription(
            # info=info,
            observable=observable,
        )
