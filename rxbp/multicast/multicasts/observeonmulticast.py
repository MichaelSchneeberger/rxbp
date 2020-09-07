from dataclasses import dataclass

from rxbp.multicast.mixins.multicastmixin import MultiCastMixin
from rxbp.multicast.multicastobservables.observeonmulticastobservable import ObserveOnMultiCastObservable
from rxbp.multicast.multicastsubscriber import MultiCastSubscriber
from rxbp.multicast.multicastsubscription import MultiCastSubscription
from rxbp.scheduler import Scheduler


@dataclass
class ObserveOnMultiCast(MultiCastMixin):
    source: MultiCastMixin
    scheduler: Scheduler

    def unsafe_subscribe(
            self,
            subscriber: MultiCastSubscriber,
    ) -> MultiCastSubscription:
        subscription = self.source.unsafe_subscribe(subscriber=subscriber)
        return subscription.copy(observable=ObserveOnMultiCastObservable(
            source=subscription.observable,
            scheduler=self.scheduler,
        ))

