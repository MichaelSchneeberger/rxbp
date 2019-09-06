from rxbp.flowablebase import FlowableBase
from rxbp.observables.pairwiseobservable import PairwiseObservable
from rxbp.selectors.bases import PairwiseBase, Base
from rxbp.subscriber import Subscriber


class PairwiseFlowable(FlowableBase):
    def __init__(self, source: FlowableBase):
        if isinstance(source.base, Base):
            base = PairwiseBase(source.base)
        else:
            base = None
        super().__init__(base=base)

        self._source = source

    def unsafe_subscribe(self, subscriber: Subscriber):
        source_obs, selectors = self._source.unsafe_subscribe(subscriber=subscriber)
        obs = PairwiseObservable(source=source_obs)

        return obs, selectors