from dataclasses import dataclass
from typing import Callable, Any

from rx.disposable import Disposable

from rxbp.multicast.mixins.multicastobservablemixin import MultiCastObservableMixin
from rxbp.multicast.mixins.multicastobservermixin import MultiCastObserverMixin
from rxbp.multicast.multicastobserverinfo import MultiCastObserverInfo
from rxbp.multicast.typing import MultiCastValue


@dataclass
class DebugMultiCastObservable(MultiCastObservableMixin):
    source: MultiCastObservableMixin
    name: str
    on_next: Callable[[Any], None]
    on_completed: Callable[[], None]
    on_error: Callable[[Exception], None]

    def observe(self, observer_info: MultiCastObserverInfo) -> Disposable:
        @dataclass
        class DebugMultiCastObserver(MultiCastObserverMixin):
            source: MultiCastObserverMixin
            on_next_debug: Callable[[Any], None]
            on_completed_debug: Callable[[], None]
            on_error_debug: Callable[[Exception], None]

            def on_next(self, elem: MultiCastValue) -> None:
                self.on_next_debug(elem)
                self.source.on_next(elem)

            def on_error(self, exc: Exception) -> None:
                self.on_error_debug(exc)
                self.source.on_error(exc)

            def on_completed(self) -> None:
                self.on_completed_debug()
                self.source.on_completed()

        observer = DebugMultiCastObserver(
            source=observer_info.observer,
            on_next_debug=self.on_next,
            on_completed_debug=self.on_completed,
            on_error_debug=self.on_error,
        )

        return self.source.observe(observer_info.copy(observer))
