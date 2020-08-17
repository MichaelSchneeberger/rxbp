from rxbp.mixins.flowablebasemixin import FlowableBaseMixin
from rxbp.observables.repeatfirstobservable import RepeatFirstObservable
from rxbp.subscriber import Subscriber
from rxbp.subscription import Subscription


class RepeatFirstFlowable(FlowableBaseMixin):
    def __init__(self, source: FlowableBaseMixin):
        # unknown base, depends on the back-pressure
        base = None

        super().__init__()

        self._source = source

    def unsafe_subscribe(self, subscriber: Subscriber) -> Subscription:
        subscription = self._source.unsafe_subscribe(subscriber=subscriber)
        observable = RepeatFirstObservable(source=subscription.observable, scheduler=subscriber.scheduler)
        return init_subscription(info=subscription.info, observable=observable)