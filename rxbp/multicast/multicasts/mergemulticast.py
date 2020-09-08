from dataclasses import dataclass
from typing import Iterable

from rxbp.multicast.mixins.multicastmixin import MultiCastMixin
from rxbp.multicast.multicastobservables.mergemulticastobservable import MergeMultiCastObservable
from rxbp.multicast.multicastsubscriber import MultiCastSubscriber
from rxbp.multicast.multicastsubscription import MultiCastSubscription


@dataclass
class MergeMultiCast(MultiCastMixin):
    sources: Iterable[MultiCastMixin]

    def unsafe_subscribe(self, subscriber: MultiCastSubscriber) -> MultiCastSubscription:
        subscriptions = [source.unsafe_subscribe(subscriber=subscriber) for source in self.sources]

        return subscriptions[0].copy(
            observable=MergeMultiCastObservable(
                sources=[s.observable for s in subscriptions],
                # scheduler=subscriber.multicast_scheduler,
            ),
        )
