from dataclasses import dataclass

from rxbp.multicast.init.initmulticastsubscription import init_multicast_subscription
from rxbp.multicast.mixins.multicastmixin import MultiCastMixin
from rxbp.multicast.multicastobservables.emptymulticastobservable import EmptyMultiCastObservable
from rxbp.multicast.multicastsubscriber import MultiCastSubscriber
from rxbp.multicast.multicastsubscription import MultiCastSubscription


@dataclass
class EmptyMultiCast(MultiCastMixin):
    scheduler_index: int

    def unsafe_subscribe(self, subscriber: MultiCastSubscriber) -> MultiCastSubscription:
        return init_multicast_subscription(EmptyMultiCastObservable(
            subscriber=subscriber,
            scheduler_index=self.scheduler_index,
        ))
