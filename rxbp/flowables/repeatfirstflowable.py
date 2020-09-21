from dataclasses import dataclass

from rxbp.mixins.flowablemixin import FlowableMixin
from rxbp.observables.repeatfirstobservable import RepeatFirstObservable
from rxbp.subscriber import Subscriber
from rxbp.subscription import Subscription


@dataclass
class RepeatFirstFlowable(FlowableMixin):
    source: FlowableMixin

    def unsafe_subscribe(self, subscriber: Subscriber) -> Subscription:
        subscription = self.source.unsafe_subscribe(subscriber=subscriber)
        return subscription.copy(
            observable=RepeatFirstObservable(
                source=subscription.observable,
                scheduler=subscriber.scheduler,
            ),
        )