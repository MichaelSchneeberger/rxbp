from typing import Callable, Iterator

import rx
from rx import Observable

from rxbp.multicast.multicastInfo import MultiCastInfo
from rxbp.multicast.mixins.multicastmixin import MultiCastMixin
from rxbp.multicast.typing import MultiCastValue


class MapToIteratorMultiCast(MultiCastMixin):
    def __init__(
            self,
            source: MultiCastMixin,
            func: Callable[[MultiCastValue], Iterator[MultiCastValue]],
    ):
        self.source = source
        self.func = func

    def get_source(self, info: MultiCastInfo) -> rx.typing.Observable:
        source = self.source.get_source(info=info)

        def subscribe(observer, scheduler=None):

            def on_next(value) -> None:
                for val in self.func(value):
                    observer.on_next(val)

            return source.subscribe_(on_next, observer.on_error, observer.on_completed, scheduler)

        return Observable(subscribe)
