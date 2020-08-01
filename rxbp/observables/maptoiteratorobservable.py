from typing import Callable, Any, Iterator

from rxbp.ack.stopack import stop_ack
from rxbp.observable import Observable
from rxbp.observer import Observer
from rxbp.observerinfo import ObserverInfo
from rxbp.typing import ElementType


class MapToIteratorObservable(Observable):
    def __init__(
            self,
            source: Observable,
            func: Callable[[Any], Iterator[Any]],
    ):
        super().__init__()

        self.source = source
        self.func = func

    def observe(self, observer_info: ObserverInfo):
        observer_info = observer_info.observer
        func = self.func

        class MapToIteratorObserver(Observer):
            def on_next(self, elem: ElementType):
                def map_gen():
                    for v in elem:
                        yield from func(v)

                try:
                    buffer = list(map_gen())
                except Exception as exc:
                    observer_info.on_error(exc)
                    return stop_ack

                return observer_info.on_next(buffer)

            def on_error(self, exc):
                return observer_info.on_error(exc)

            def on_completed(self):
                return observer_info.on_completed()

        map_subscription = observer_info.copy(MapToIteratorObserver())
        return self.source.observe(map_subscription)
