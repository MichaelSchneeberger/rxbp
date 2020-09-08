from dataclasses import dataclass
from traceback import FrameSummary
from typing import Callable, Any, List

from rxbp.multicast.mixins.multicastmixin import MultiCastMixin
from rxbp.multicast.multicastobservables.debugmulticastobservable import DebugMultiCastObservable
from rxbp.multicast.multicastobserverinfo import MultiCastObserverInfo
from rxbp.multicast.multicastsubscriber import MultiCastSubscriber
from rxbp.multicast.multicastsubscription import MultiCastSubscription


@dataclass
class DebugMultiCast(MultiCastMixin):
    source: MultiCastMixin
    name: str
    on_next: Callable[[Any], None]
    on_completed: Callable[[], None]
    on_error: Callable[[Exception], None]
    on_subscribe: Callable[[MultiCastSubscriber], None]
    on_observe: Callable[[MultiCastObserverInfo], None]
    on_dispose: Callable[[], None]
    stack: List[FrameSummary]

    def unsafe_subscribe(self, subscriber: MultiCastSubscriber) -> MultiCastSubscription:
        self.on_subscribe(subscriber)
        subscription = self.source.unsafe_subscribe(subscriber=subscriber)

        return subscription.copy(
            observable=DebugMultiCastObservable(
                source=subscription.observable,
                name=self.name,
                on_next=self.on_next,
                on_error=self.on_error,
                on_completed=self.on_completed,
                on_observe=self.on_observe,
                on_dispose=self.on_dispose,
                stack=self.stack,
            ),
        )
