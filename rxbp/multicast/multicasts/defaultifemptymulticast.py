from typing import Any, Callable

import rx
from rx import Observable
from rx.disposable import Disposable

from rxbp.multicast.mixins.multicastmixin import MultiCastMixin
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
        def default_if_empty() -> Callable[[Observable], Observable]:
            def func(source: Observable) -> Observable:
                def subscribe(observer, scheduler=None) -> Disposable:
                    found = [False]

                    def on_next(x: Any):
                        found[0] = True
                        observer.on_next(x)

                    def on_completed():
                        if not found[0]:
                            observer.on_next(self.lazy_val())
                        observer.on_completed()

                    return source.subscribe_(on_next, observer.on_error, on_completed, scheduler)

                return Observable(subscribe)

            return func

        return self.source.get_source(info=info).pipe(
            default_if_empty(),
        )
