from dataclasses import dataclass
from typing import Iterable, Any

from rxbp.multicast.init.initmulticastsubscription import init_multicast_subscription
from rxbp.multicast.mixins.multicastmixin import MultiCastMixin
from rxbp.multicast.multicastobservables.fromiterableobservable import FromIterableObservable
from rxbp.multicast.multicastsubscriber import MultiCastSubscriber
from rxbp.multicast.multicastsubscription import MultiCastSubscription


@dataclass
class FromIterableMultiCast(MultiCastMixin):
    values: Iterable[Any]

    def unsafe_subscribe(self, subscriber: MultiCastSubscriber) -> MultiCastSubscription:
        return init_multicast_subscription(FromIterableObservable(
            values=self.values,
            subscribe_scheduler=subscriber.multicast_scheduler,
        ))