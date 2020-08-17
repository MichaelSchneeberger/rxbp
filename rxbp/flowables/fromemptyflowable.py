from rxbp.init.initsubscription import init_subscription
from rxbp.mixins.flowablebasemixin import FlowableBaseMixin
from rxbp.observables.fromemptyobservable import FromEmptyObservable
from rxbp.subscriber import Subscriber
from rxbp.subscription import Subscription


class FromEmptyFlowable(FlowableBaseMixin):
    def unsafe_subscribe(self, subscriber: Subscriber) -> Subscription:
        return init_subscription(
            observable=FromEmptyObservable(
                subscribe_scheduler=subscriber.subscribe_scheduler,
            ),
        )
