from typing import Callable, Iterator

import rx
from rx import Observable

from rxbp.multicast.mixins.multicastmixin import MultiCastMixin
from rxbp.multicast.multicastsubscriber import MultiCastSubscriber
from rxbp.multicast.typing import MultiCastItem


class MapToIteratorMultiCast(MultiCastMixin):
    def __init__(
            self,
            source: MultiCastMixin,
            func: Callable[[MultiCastItem], Iterator[MultiCastItem]],
    ):
        self.source = source
        self.func = func

    def unsafe_subscribe(self, subscriber: MultiCastSubscriber) -> rx.typing.Observable:
        source = self.source.get_source(info=info)

        def subscribe(observer, scheduler=None):

            def on_next(value) -> None:
                for val in self.func(value):
                    observer.on_next(val)

            return source.subscribe_(on_next, observer.on_error, observer.on_completed, scheduler)

        return Observable(subscriber)
