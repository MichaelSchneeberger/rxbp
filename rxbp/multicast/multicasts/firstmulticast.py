from dataclasses import dataclass
from traceback import FrameSummary
from typing import List

from rxbp.multicast.mixins.multicastmixin import MultiCastMixin
from rxbp.multicast.multicastobservables.firstmulticastobservable import FirstMultiCastObservable
from rxbp.multicast.multicastsubscriber import MultiCastSubscriber
from rxbp.multicast.multicastsubscription import MultiCastSubscription


@dataclass
class FirstMultiCast(MultiCastMixin):
    source: MultiCastMixin
    stack: List[FrameSummary]

    def unsafe_subscribe(self, subscriber: MultiCastSubscriber) -> MultiCastSubscription:
        subscription = self.source.unsafe_subscribe(subscriber=subscriber)
        return subscription.copy(
            observable=FirstMultiCastObservable(
                source=subscription.observable,
                stack=self.stack,
            )
        )
