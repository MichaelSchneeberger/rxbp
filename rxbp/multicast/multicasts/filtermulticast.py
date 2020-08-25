from dataclasses import dataclass
from typing import Callable

import rx

from rxbp.multicast.mixins.multicastmixin import MultiCastMixin
from rxbp.multicast.multicastobservables.filtermulticastobservable import FilterMultiCastObservable
from rxbp.multicast.multicastsubscriber import MultiCastSubscriber
from rxbp.multicast.typing import MultiCastItem


@dataclass
class FilterMultiCast(MultiCastMixin):
    source: MultiCastMixin
    predicate: Callable[[MultiCastItem], bool]

    def unsafe_subscribe(self, subscriber: MultiCastSubscriber) -> rx.typing.Observable:
        subscription = self.source.unsafe_subscribe(subscriber=subscriber)
        return subscription.copy(
            observable=FilterMultiCastObservable(
                source=subscription.observable,
                predicate=self.predicate,
            )
        )
