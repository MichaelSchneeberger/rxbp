from typing import Callable

from rxbp.multicast.mixins.multicastmixin import MultiCastMixin
from rxbp.multicast.multicastobservables.mapmulticastobservable import MapMultiCastObservable
from rxbp.multicast.multicastsubscriber import MultiCastSubscriber
from rxbp.multicast.multicastsubscription import MultiCastSubscription
from rxbp.multicast.typing import MultiCastItem


class MapMultiCast(MultiCastMixin):
    def __init__(
            self,
            source: MultiCastMixin,
            func: Callable[[MultiCastItem], MultiCastItem],
    ):
        self.source = source
        self.func = func

    def unsafe_subscribe(self, subscriber: MultiCastSubscriber) -> MultiCastSubscription:
        subscription = self.source.unsafe_subscribe(subscriber=subscriber)
        return subscription.copy(observable=MapMultiCastObservable(
            source=subscription.observable,
            func=self.func,
        ))
