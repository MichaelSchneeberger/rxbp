from dataclasses import dataclass
from typing import Callable

from rx.disposable import Disposable

from rxbp.multicast.init.initmulticastsubscription import init_multicast_subscription
from rxbp.multicast.mixins.multicastmixin import MultiCastMixin
from rxbp.multicast.multicastobservable import MultiCastObservable
from rxbp.multicast.multicastobserverinfo import MultiCastObserverInfo
from rxbp.multicast.multicasts.sharedmulticast import SharedMultiCast
from rxbp.multicast.multicastsubscriber import MultiCastSubscriber
from rxbp.multicast.multicastsubscription import MultiCastSubscription
from rxbp.multicast.typing import MultiCastItem


@dataclass
class LiftMultiCast(MultiCastMixin):
    source: SharedMultiCast
    func: Callable[[MultiCastMixin], MultiCastItem]

    def unsafe_subscribe(self, subscriber: MultiCastSubscriber) -> MultiCastSubscription:

        outer_self = self

        class LiftObservable(MultiCastObservable):
            def observe(self, observer_info: MultiCastObserverInfo) -> Disposable:
                def action(_, __):
                    observer_info.observer.on_next([outer_self.func(outer_self.source)])
                    observer_info.observer.on_completed()

                    # subscription = outer_self.source.unsafe_subscribe(subscriber=subscriber)
                    # return subscription.observable.observe(observer_info)

                return subscriber.multicast_scheduler.schedule(action)

        return init_multicast_subscription(LiftObservable())

        # if self.shared_observable is None:
        #     class InnerLiftMultiCast(MultiCastMixin):
        #         def __init__(self, source: rx.typing.Observable[MultiCastItem]):
        #             self._source = source
        #
        #         def unsafe_subscribe(self, subscriber: MultiCastSubscriber) -> rx.typing.Observable:
        #             return self._source
        #
        #     def lift_func(obs: rx.typing.Observable, first: MultiCastItem):
        #         source = obs.pipe(
        #             rxop.share(),
        #         )
        #         inner_multicast = InnerLiftMultiCast(source=source)
        #         multicast_val = self.func(inner_multicast, first)
        #         return multicast_val.get_source(info=info)
        #
        #     self.shared_observable = LiftObservable(
        #         source=self.source.get_source(info=info),
        #         func=lift_func,
        #         scheduler=info.multicast_scheduler,
        #     ).pipe(
        #         rxop.map(lambda obs: InnerLiftMultiCast(obs)),
        #         rxop.share(),
        #     )
        #
        # return self.shared_observable
