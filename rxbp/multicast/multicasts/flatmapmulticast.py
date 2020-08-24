from typing import Callable

import rx
from rx import operators as rxop

from rxbp.multicast.multicastobservables.flatmapmulticastobservable import FlatMapMultiCastObservable
from rxbp.multicast.multicastsubscriber import MultiCastSubscriber
from rxbp.multicast.mixins.multicastmixin import MultiCastMixin
from rxbp.multicast.multicastsubscription import MultiCastSubscription
from rxbp.multicast.typing import MultiCastItem


class FlatMapMultiCast(MultiCastMixin):
    def __init__(
            self,
            source: MultiCastMixin,
            func: Callable[[MultiCastItem], MultiCastMixin],
    ):
        self.source = source
        self.func = func

    def unsafe_subscribe(self, subscriber: MultiCastSubscriber) -> MultiCastSubscription:
        subscription = self.source.unsafe_subscribe(subscriber=subscriber)

        def lifted_func(val):
            multicast = self.func(val)
            subscription = multicast.unsafe_subscribe(subscriber=subscriber)
            return subscription.observable

        return subscription.copy(
            observable=FlatMapMultiCastObservable(
                source=subscription.observable,
                func=lifted_func,
            )
        )

        # def check_return_value_of_func(value):
        #     multi_cast = self.func(value)
        #
        #     if not isinstance(multi_cast, MultiCastMixin):
        #         raise Exception(f'"{self.func}" should return a "MultiCast", but returned "{multi_cast}"')
        #
        #     return multi_cast.get_source(info=info)
        #
        # return self.source.get_source(info=info).pipe(
        #     rxop.flat_map(check_return_value_of_func),
        # )
