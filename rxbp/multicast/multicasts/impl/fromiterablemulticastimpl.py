from rxbp.multicast.mixins.multicastmixin import MultiCastMixin
from rxbp.multicast.multicastsubscriber import MultiCastSubscriber
from rxbp.multicast.multicastsubscription import MultiCastSubscription


class FromIterableMultiCast(MultiCastMixin):
    def unsafe_subscribe(self, subscriber: MultiCastSubscriber) -> MultiCastSubscription:
        return rx.from_(vals, scheduler=subscriber.multicast_scheduler)