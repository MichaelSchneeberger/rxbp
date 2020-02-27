from typing import Any, Callable

import rx
from rx import operators as rxop, Observable
from rx.disposable import Disposable

from rxbp.multicast.multicastInfo import MultiCastInfo
from rxbp.multicast.multicastbase import MultiCastBase
from rxbp.multicast.typing import MultiCastValue


class DefaultIfEmptyMultiCast(MultiCastBase):
    def __init__(
            self,
            source: MultiCastBase,
            lazy_val: Callable[[], Any],
    ):
        self.source = source
        self.lazy_val = lazy_val

    def get_source(self, info: MultiCastInfo) -> rx.typing.Observable[MultiCastValue]:
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
