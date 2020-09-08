from dataclasses import dataclass
from typing import Optional

from rxbp.multicast.mixins.multicastmixin import MultiCastMixin
from rxbp.multicast.multicastobservables.observeonmulticastobservable import ObserveOnMultiCastObservable
from rxbp.multicast.multicastsubscriber import MultiCastSubscriber
from rxbp.multicast.multicastsubscription import MultiCastSubscription
from rxbp.scheduler import Scheduler


@dataclass
class ObserveOnMultiCast(MultiCastMixin):
    source: MultiCastMixin
    scheduler: Optional[Scheduler]

    def unsafe_subscribe(
            self,
            subscriber: MultiCastSubscriber,
    ) -> MultiCastSubscription:
        scheduler = self.scheduler or subscriber.source_scheduler

        subscription = self.source.unsafe_subscribe(subscriber=subscriber)
        return subscription.copy(observable=ObserveOnMultiCastObservable(
            source=subscription.observable,
            scheduler=scheduler,
            source_scheduler=subscriber.source_scheduler,
        ))

