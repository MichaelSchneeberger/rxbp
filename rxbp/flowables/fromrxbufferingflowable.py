from dataclasses import dataclass

import rx

from rxbp.init.initsubscription import init_subscription
from rxbp.mixins.flowablemixin import FlowableMixin
from rxbp.observables.fromrxbufferingobservable import FromRxBufferingObservable
from rxbp.overflowstrategy import OverflowStrategy
from rxbp.subscriber import Subscriber
from rxbp.subscription import Subscription


@dataclass
class FromRxBufferingFlowable(FlowableMixin):
    batched_source: rx.typing.Observable
    overflow_strategy: OverflowStrategy
    buffer_size: int

    def unsafe_subscribe(self, subscriber: Subscriber) -> Subscription:
        return init_subscription(
            observable=FromRxBufferingObservable(
                batched_source=self.batched_source,
                scheduler=subscriber.scheduler,
                subscribe_scheduler=subscriber.subscribe_scheduler,
                overflow_strategy=self.overflow_strategy,
                buffer_size=self.buffer_size,
            ),
        )
