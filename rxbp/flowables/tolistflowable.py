from rxbp.flowablebase import FlowableBase
from rxbp.observables.tolistobservable import ToListObservable
from rxbp.selectors.bases import NumericalBase
from rxbp.subscriber import Subscriber
from rxbp.subscription import Subscription


class ToListFlowable(FlowableBase):
    def __init__(self, source: FlowableBase):
        # to_list emits exactly one element
        base = NumericalBase(1)

        super().__init__(base=base, selectable_bases=source.selectable_bases)

        self._source = source

    def unsafe_subscribe(self, subscriber: Subscriber) -> Subscription:
        source_observable, source_selectors = self._source.unsafe_subscribe(subscriber=subscriber)
        obs = ToListObservable(source=source_observable)
        return obs, source_selectors