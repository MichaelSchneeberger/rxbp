from rxbp.flowablebase import FlowableBase
from rxbp.observables.delaysubscriptionobservable import DelaySubscriptionObservable
from rxbp.subscriber import Subscriber
from rxbp.subscription import Subscription


class DelaySubscriptionFlowable(FlowableBase):
    def __init__(self, source: FlowableBase):
        super().__init__()

        self.source = source

    def unsafe_subscribe(self, subscriber: Subscriber) -> Subscription:
        subscription = self.source.unsafe_subscribe(subscriber=subscriber)

        observable = DelaySubscriptionObservable(
            source=subscription.observable,
            subscribe_scheduler=subscriber.subscribe_scheduler,
        )

        return Subscription(info=subscription.info, observable=observable)
