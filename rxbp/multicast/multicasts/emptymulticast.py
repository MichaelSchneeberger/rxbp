from rxbp.multicast.init.initmulticastsubscription import init_multicast_subscription
from rxbp.multicast.mixins.multicastmixin import MultiCastMixin
from rxbp.multicast.multicastobservables.emptymulticastobservable import EmptyMultiCastObservable
from rxbp.multicast.multicastsubscriber import MultiCastSubscriber
from rxbp.multicast.multicastsubscription import MultiCastSubscription


class EmptyMultiCast(MultiCastMixin):
    def unsafe_subscribe(self, subscriber: MultiCastSubscriber) -> MultiCastSubscription:
        return init_multicast_subscription(EmptyMultiCastObservable(
            source_scheduler=subscriber.source_scheduler,
        ))
