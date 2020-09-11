from dataclasses import dataclass

from rxbp.flowable import Flowable
from rxbp.init.initsubscriber import init_subscriber
from rxbp.multicast.init.initmulticastsubscription import init_multicast_subscription
from rxbp.multicast.mixins.multicastmixin import MultiCastMixin
from rxbp.multicast.multicastobservables.fromflowablemulticastobservable import FromFlowableMultiCastObservable
from rxbp.multicast.multicastsubscriber import MultiCastSubscriber
from rxbp.multicast.multicastsubscription import MultiCastSubscription


@dataclass
class FromFlowableMultiCast(MultiCastMixin):
    source: Flowable

    def unsafe_subscribe(self, subscriber: MultiCastSubscriber) -> MultiCastSubscription:
        subscription = self.source.unsafe_subscribe(init_subscriber(
            scheduler=subscriber.subscribe_schedulers[0],
            subscribe_scheduler=subscriber.subscribe_schedulers[0],
        ))
        return init_multicast_subscription(
            observable=FromFlowableMultiCastObservable(
                source=subscription.observable,
                subscriber=subscriber,
            ),
        )