from dataclasses import dataclass
from typing import Callable, Any

import rx
from rx import Observable

from rxbp.multicast.mixins.multicastmixin import MultiCastMixin
from rxbp.multicast.multicastsubscriber import MultiCastSubscriber


@dataclass
class FirstOrDefaultMultiCast(MultiCastMixin):
    source: MultiCastMixin
    lazy_val: Callable[[], Any]

    def unsafe_subscribe(self, subscriber: MultiCastSubscriber) -> rx.typing.Observable:
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

        return Observable(subscriber)
