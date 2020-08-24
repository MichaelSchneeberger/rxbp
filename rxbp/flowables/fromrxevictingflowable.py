from dataclasses import dataclass

import rx

from rxbp.init.initsubscription import init_subscription
from rxbp.mixins.flowablemixin import FlowableMixin
from rxbp.observables.fromrxevictingobservable import FromRxEvictingObservable
from rxbp.overflowstrategy import OverflowStrategy
from rxbp.subscriber import Subscriber
from rxbp.subscription import Subscription


@dataclass
class FromRxEvictingFlowable(FlowableMixin):
    batched_source: rx.typing.Observable
    overflow_strategy: OverflowStrategy

    def unsafe_subscribe(self, subscriber: Subscriber) -> Subscription:
        return init_subscription(
            observable=FromRxEvictingObservable(
                batched_source=self.batched_source,
                scheduler=subscriber.scheduler,
                subscribe_scheduler=subscriber.subscribe_scheduler,
                overflow_strategy=self.overflow_strategy,
            ),
        )
