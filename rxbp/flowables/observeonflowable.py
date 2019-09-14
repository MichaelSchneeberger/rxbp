from rxbp.flowablebase import FlowableBase
from rxbp.observables.observeonobservable import ObserveOnObservable
from rxbp.scheduler import Scheduler
from rxbp.subscriber import Subscriber
from rxbp.subscription import Subscription


class ObserveOnFlowable(FlowableBase):
    def __init__(self, source: FlowableBase, scheduler: Scheduler):
        super().__init__()

        self._source = source
        self._scheduler = scheduler

    def unsafe_subscribe(self, subscriber: Subscriber):
        subscription = self._source.unsafe_subscribe(subscriber=subscriber)
        observable = ObserveOnObservable(source=subscription.observable, scheduler=self._scheduler)

        return Subscription(subscription.info, observable=observable)