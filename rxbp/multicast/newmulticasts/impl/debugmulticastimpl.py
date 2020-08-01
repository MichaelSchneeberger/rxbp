from dataclasses import dataclass
from typing import Callable, Any

from rxbp.multicast.mixins.multicastmixin import MultiCastMixin
from rxbp.multicast.multicastobservables.debugmulticastobservable import DebugMultiCastObservable
from rxbp.multicast.multicastsubscriber import MultiCastSubscriber
from rxbp.multicast.multicastsubscription import MultiCastSubscription


@dataclass
class DebugMultiCastImpl(MultiCastMixin):
    source: MultiCastMixin
    name: str
    on_next: Callable[[Any], None]
    on_completed: Callable[[], None]
    on_error: Callable[[Exception], None]

    def unsafe_subscribe(self, subscriber: MultiCastSubscriber) -> MultiCastSubscription:
        subscription = self.source.unsafe_subscribe(subscriber=subscriber)

        observable = DebugMultiCastObservable(
            source=subscription.observable,
            name=self.name,
            on_next=self.on_next,
            on_error=self.on_error,
            on_completed=self.on_completed,
        )

        return subscription.copy(observable=observable)
