from rxbp.mixins.flowablemixin import FlowableMixin
from rxbp.observables.mergeobservable import MergeObservable
from rxbp.subscriber import Subscriber


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

        return left_subscription.copy(
            observable=observable,
        )
