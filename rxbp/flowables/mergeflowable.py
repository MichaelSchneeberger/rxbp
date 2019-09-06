from rxbp.flowablebase import FlowableBase
from rxbp.observables.mergeobservable import MergeObservable
from rxbp.subscriber import Subscriber


class MergeFlowable(FlowableBase):
    def __init__(self, source: FlowableBase, other: FlowableBase):
        # the base becomes anonymous after merging
        base = None

        super().__init__(base=base)

        self._source = source
        self._other = other

    def unsafe_subscribe(self, subscriber: Subscriber):
        left_obs, _ = self._source.unsafe_subscribe(subscriber=subscriber)
        right_obs, _ = self._other.unsafe_subscribe(subscriber=subscriber)
        obs = MergeObservable(left=left_obs, right=right_obs)

        return obs, {}