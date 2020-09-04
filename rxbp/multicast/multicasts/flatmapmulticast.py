from dataclasses import dataclass
from traceback import FrameSummary
from typing import Callable, List

from rxbp.multicast.mixins.multicastmixin import MultiCastMixin
from rxbp.multicast.multicastobservables.flatmapmulticastobservable import FlatMapMultiCastObservable
from rxbp.multicast.multicastsubscriber import MultiCastSubscriber
from rxbp.multicast.multicastsubscription import MultiCastSubscription
from rxbp.multicast.typing import MultiCastItem
from rxbp.utils.tooperatorexception import to_operator_exception


@dataclass
class FlatMapMultiCast(MultiCastMixin):
    source: MultiCastMixin
    func: Callable[[MultiCastItem], MultiCastMixin]
    stack: List[FrameSummary]

    def unsafe_subscribe(self, subscriber: MultiCastSubscriber) -> MultiCastSubscription:
        subscription = self.source.unsafe_subscribe(subscriber=subscriber)

        def lifted_func(val):
            multicast = self.func(val)

            if not isinstance(multicast, MultiCastMixin):
                raise Exception(to_operator_exception(
                    message=f'"{multicast}" should be of type MultiCastMixin',
                    stack=self.stack,
                ))

            subscription = multicast.unsafe_subscribe(subscriber=subscriber)

            return subscription.observable

        return subscription.copy(
            observable=FlatMapMultiCastObservable(
                source=subscription.observable,
                func=lifted_func,
                multicast_scheduler=subscriber.multicast_scheduler,
            )
        )
