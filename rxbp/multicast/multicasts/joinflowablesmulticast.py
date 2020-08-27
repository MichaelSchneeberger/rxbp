from dataclasses import dataclass
from typing import List

from rxbp.multicast.init.initmulticastsubscription import init_multicast_subscription
from rxbp.multicast.mixins.multicastmixin import MultiCastMixin
from rxbp.multicast.multicastobservables.joinflowablesmulticastobservable import JoinFlowableMultiCastObservable
from rxbp.multicast.multicastsubscriber import MultiCastSubscriber
from rxbp.multicast.multicastsubscription import MultiCastSubscription


@dataclass
class JoinFlowablesMultiCast(MultiCastMixin):
    sources: List[MultiCastMixin]

    def unsafe_subscribe(self, subscriber: MultiCastSubscriber) -> MultiCastSubscription:
        def gen_observables():
            for source in self.sources:
                yield source.unsafe_subscribe(MultiCastSubscriber(
                    multicast_scheduler=subscriber.multicast_scheduler,
                    source_scheduler=subscriber.multicast_scheduler,
                )).observable

        return init_multicast_subscription(
            observable=JoinFlowableMultiCastObservable(
                sources=list(gen_observables()),
                multicast_scheduler=subscriber.multicast_scheduler,
                source_scheduler=subscriber.source_scheduler,
            ),
        )
