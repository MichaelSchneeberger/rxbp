from rxbp.mixins.flowablebasemixin import FlowableBaseMixin
from rxbp.observables.observeonobservable import ObserveOnObservable
from rxbp.scheduler import Scheduler
from rxbp.subscriber import Subscriber
from rxbp.subscription import Subscription


class ObserveOnFlowable(FlowableBaseMixin):
    def __init__(self, source: FlowableBaseMixin, scheduler: Scheduler):
        super().__init__()

        self._source = source
        self._scheduler = scheduler

    def unsafe_subscribe(self, subscriber: Subscriber):
        subscription = self._source.unsafe_subscribe(subscriber=subscriber)
        observable = ObserveOnObservable(source=subscription.observable, scheduler=self._scheduler)

        return init_subscription(subscription.info, observable=observable)