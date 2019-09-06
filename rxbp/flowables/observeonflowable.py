from rxbp.flowablebase import FlowableBase
from rxbp.observables.observeonobservable import ObserveOnObservable
from rxbp.scheduler import Scheduler
from rxbp.subscriber import Subscriber


class ObserveOnFlowable(FlowableBase):
    def __init__(self, source: FlowableBase, scheduler: Scheduler):
        super().__init__(base=source.base, selectable_bases=source.selectable_bases)

        self._source = source
        self._scheduler = scheduler

    def unsafe_subscribe(self, subscriber: Subscriber):
        source_obs, selectors = self._source.unsafe_subscribe(subscriber=subscriber)
        obs = ObserveOnObservable(source=source_obs, scheduler=self._scheduler)

        return obs, selectors