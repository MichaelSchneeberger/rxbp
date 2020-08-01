from typing import Callable

import rx
from rx import operators as rxop

from rxbp.multicast.init.initmulticastsubscription import init_multicast_subscription
from rxbp.multicast.multicastInfo import MultiCastInfo
from rxbp.multicast.mixins.multicastmixin import MultiCastMixin
from rxbp.multicast.multicastobservables.mapmulticastobservable import MapMultiCastObservable
from rxbp.multicast.multicastsubscriber import MultiCastSubscriber
from rxbp.multicast.multicastsubscription import MultiCastSubscription
from rxbp.multicast.typing import MultiCastValue


class MapMultiCast(MultiCastMixin):
    def __init__(
            self,
            source: MultiCastMixin,
            func: Callable[[MultiCastValue], MultiCastValue],
    ):
        self.source = source
        self.func = func

    def unsafe_subscribe(self, subscriber: MultiCastSubscriber) -> MultiCastSubscription:
        source = self.source.unsafe_subscribe(subscriber=subscriber).observable

        observable = MapMultiCastObservable(
            source=source,
            func=self.func,
        )

        return init_multicast_subscription(observable=observable)
