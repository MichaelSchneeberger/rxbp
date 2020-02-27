from dataclasses import dataclass
from typing import Callable, Any

import rx
from rx import Observable
from rx.internal import SequenceContainsNoElementsError

from rxbp.multicast.multicastInfo import MultiCastInfo
from rxbp.multicast.multicastbase import MultiCastBase


@dataclass
class FirstOrDefaultMultiCast(MultiCastBase):
    source: MultiCastBase
    lazy_val: Callable[[], Any]

    def get_source(self, info: MultiCastInfo) -> rx.typing.Observable:
        source = self.source.get_source(info=info)

        def subscribe(observer, scheduler=None):
            def on_next(x):
                observer.on_next(x)
                observer.on_completed()

            def on_completed():
                try:
                    observer.on_next(self.lazy_val())
                    observer.on_completed()
                except Exception as exc:
                    observer.on_error(exc)

            return source.subscribe_(on_next, observer.on_error, on_completed, scheduler)

        return Observable(subscribe)
