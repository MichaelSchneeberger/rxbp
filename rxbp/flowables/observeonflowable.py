from rxbp.mixins.flowablemixin import FlowableMixin
from rxbp.observables.observeonobservable import ObserveOnObservable
from rxbp.scheduler import Scheduler
from rxbp.subscriber import Subscriber


class ObserveOnFlowable(FlowableMixin):
    def __init__(self, source: FlowableMixin, scheduler: Scheduler):
        super().__init__()

        self._source = source
        self._scheduler = scheduler

    def unsafe_subscribe(self, subscriber: Subscriber):
        subscription = self._source.unsafe_subscribe(subscriber=subscriber)

        return subscription.copy(observable=ObserveOnObservable(
            source=subscription.observable,
            scheduler=self._scheduler,
        ))
