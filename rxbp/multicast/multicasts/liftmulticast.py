from dataclasses import dataclass

from rx.disposable import Disposable

from rxbp.multicast.init.initmulticastsubscription import init_multicast_subscription
from rxbp.multicast.mixins.multicastmixin import MultiCastMixin
from rxbp.multicast.multicastobservable import MultiCastObservable
from rxbp.multicast.multicastobserverinfo import MultiCastObserverInfo
from rxbp.multicast.multicasts.sharedmulticast import SharedMultiCast
from rxbp.multicast.multicastsubscriber import MultiCastSubscriber
from rxbp.multicast.multicastsubscription import MultiCastSubscription


@dataclass
class LiftMultiCast(MultiCastMixin):
    source: SharedMultiCast

    def unsafe_subscribe(self, subscriber: MultiCastSubscriber) -> MultiCastSubscription:
        @dataclass
        class LiftMultiCastObservable(MultiCastObservable):
            source: MultiCastMixin

            def observe(self, observer_info: MultiCastObserverInfo) -> Disposable:
                def action(_, __):
                    try:
                        observer_info.observer.on_next([self.source])
                        observer_info.observer.on_completed()

                    except Exception as exc:
                        observer_info.observer.on_error(exc)

                return subscriber.multicast_scheduler.schedule(action)

        return init_multicast_subscription(LiftMultiCastObservable(
            source=self.source,
        ))
