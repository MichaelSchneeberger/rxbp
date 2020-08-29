from typing import Any, Callable

import rx

from rxbp.multicast.mixins.multicastmixin import MultiCastMixin
from rxbp.multicast.multicastobservables.defaultifemptymulticastobservable import DefaultIfEmptyMultiCastObservable
from rxbp.multicast.multicastsubscriber import MultiCastSubscriber
from rxbp.multicast.typing import MultiCastItem


class DefaultIfEmptyMultiCast(MultiCastMixin):
    def __init__(
            self,
            source: MultiCastMixin,
            lazy_val: Callable[[], Any],
    ):
        self.source = source
        self.lazy_val = lazy_val

    def unsafe_subscribe(self, subscriber: MultiCastSubscriber) -> rx.typing.Observable[MultiCastItem]:
        subscription = self.source.unsafe_subscribe(subscriber=subscriber)
        return subscription.copy(
            observable=DefaultIfEmptyMultiCastObservable(
                source=subscription.observable,
                lazy_val=self.lazy_val,
            ),
        )
