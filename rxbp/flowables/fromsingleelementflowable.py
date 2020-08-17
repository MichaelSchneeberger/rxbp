from dataclasses import dataclass
from typing import Callable

from rxbp.init.initsubscription import init_subscription
from rxbp.mixins.flowablebasemixin import FlowableBaseMixin
from rxbp.mixins.flowablebasemixin import FlowableBaseMixin
from rxbp.observables.fromsingleelementobservable import FromSingleElementObservable
from rxbp.subscriber import Subscriber
from rxbp.subscription import Subscription
from rxbp.typing import ElementType


@dataclass
class FromSingleElementFlowable(FlowableBaseMixin):
    lazy_elem: Callable[[], ElementType]

    def unsafe_subscribe(self, subscriber: Subscriber) -> Subscription:
        return init_subscription(
            observable=FromSingleElementObservable(
                lazy_elem=self.lazy_elem,
                subscribe_scheduler=subscriber.subscribe_scheduler,
            ),
        )
