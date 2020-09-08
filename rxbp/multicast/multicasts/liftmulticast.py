from dataclasses import dataclass

from rxbp.multicast.init.initmulticastsubscription import init_multicast_subscription
from rxbp.multicast.mixins.multicastmixin import MultiCastMixin
from rxbp.multicast.multicastobservables.fromiterableobservable import FromIterableObservable
from rxbp.multicast.multicastobservables.liftmulticastobservable import LiftMultiCastObservable
from rxbp.multicast.multicasts.sharedmulticast import SharedMultiCast
from rxbp.multicast.multicastsubscriber import MultiCastSubscriber
from rxbp.multicast.multicastsubscription import MultiCastSubscription


# @dataclass
# class LiftMultiCast(MultiCastMixin):
#     source: SharedMultiCast
#
#     def unsafe_subscribe(self, subscriber: MultiCastSubscriber) -> MultiCastSubscription:
#         # return init_multicast_subscription(LiftMultiCastObservable(
#         #     source=self.source,
#         #     # multicast_scheduler=subscriber.multicast_scheduler,
#         # ))
#         return init_multicast_subscription(FromIterableObservable(
#             values=[self.source],
#             subscriber=subscriber,
#             scheduler_index=self.scheduler_index,
#         ))
