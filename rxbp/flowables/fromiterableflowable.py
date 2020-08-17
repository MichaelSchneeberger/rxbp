from dataclasses import dataclass
from typing import Iterable

from rxbp.init.initsubscription import init_subscription
from rxbp.mixins.flowablebasemixin import FlowableBaseMixin
from rxbp.observables.fromiteratorobservable import FromIteratorObservable
from rxbp.subscriber import Subscriber
from rxbp.subscription import Subscription
from rxbp.typing import ValueType


@dataclass
class FromIterableFlowable(FlowableBaseMixin):
    iterable: Iterable[ValueType]

    def unsafe_subscribe(self, subscriber: Subscriber) -> Subscription:
        iterator = iter(self.iterable)

        return init_subscription(
            observable=FromIteratorObservable(
                iterator=iterator,
                subscribe_scheduler=subscriber.subscribe_scheduler,
                scheduler=subscriber.scheduler,
            ),
        )
