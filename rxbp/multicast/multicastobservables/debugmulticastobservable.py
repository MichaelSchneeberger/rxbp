from dataclasses import dataclass
from typing import Callable, Any

from rx.disposable import Disposable

from rxbp.multicast.multicastobservable import MultiCastObservable
from rxbp.multicast.multicastobserver import MultiCastObserver
from rxbp.multicast.multicastobserverinfo import MultiCastObserverInfo
from rxbp.multicast.multicastsubscriber import MultiCastSubscriber
from rxbp.multicast.typing import MultiCastItem


@dataclass
class DebugMultiCastObservable(MultiCastObservable):
    source: MultiCastObservable
    name: str
    on_next: Callable[[Any], None]
    on_completed: Callable[[], None]
    on_error: Callable[[Exception], None]
    on_subscribe: Callable[[MultiCastObserverInfo, MultiCastSubscriber], None]
    subscriber: MultiCastSubscriber

    def observe(self, observer_info: MultiCastObserverInfo) -> Disposable:
        self.on_subscribe(observer_info, self.subscriber)

        @dataclass
        class DebugMultiCastObserver(MultiCastObserver):
            source: MultiCastObserver
            on_next_func: Callable[[Any], None]
            on_completed_func: Callable[[], None]
            on_error_func: Callable[[Exception], None]

            def on_next(self, elem: MultiCastItem) -> None:
                elem = list(elem)
                self.on_next_func(elem)
                self.source.on_next(elem)

            def on_error(self, exc: Exception) -> None:
                self.on_error_func(exc)
                self.source.on_error(exc)

            def on_completed(self) -> None:
                self.on_completed_func()
                self.source.on_completed()

        observer = DebugMultiCastObserver(
            source=observer_info.observer,
            on_next_func=self.on_next,
            on_completed_func=self.on_completed,
            on_error_func=self.on_error,
        )

        return self.source.observe(observer_info.copy(observer))
