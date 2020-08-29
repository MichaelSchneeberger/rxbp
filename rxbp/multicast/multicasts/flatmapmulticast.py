from dataclasses import dataclass
from typing import Callable

from rxbp.multicast.mixins.multicastmixin import MultiCastMixin
from rxbp.multicast.multicastobservables.flatmapmulticastobservable import FlatMapMultiCastObservable
from rxbp.multicast.multicastsubscriber import MultiCastSubscriber
from rxbp.multicast.multicastsubscription import MultiCastSubscription
from rxbp.multicast.typing import MultiCastItem


@dataclass
class FlatMapMultiCast(MultiCastMixin):
    source: MultiCastMixin
    func: Callable[[MultiCastItem], MultiCastMixin]
    # stack: List[FrameSummary]

    def unsafe_subscribe(self, subscriber: MultiCastSubscriber) -> MultiCastSubscription:
        subscription = self.source.unsafe_subscribe(subscriber=subscriber)

        def lifted_func(val):
            multicast = self.func(val)
            subscription = multicast.unsafe_subscribe(subscriber=subscriber)

            return subscription.observable

        return subscription.copy(
            observable=FlatMapMultiCastObservable(
                source=subscription.observable,
                func=lifted_func,
                multicast_scheduler=subscriber.multicast_scheduler,
            )
        )
