from dataclasses import dataclass
from typing import Callable, Any

from rxbp.multicast.multicastobserver import MultiCastObserver
from rxbp.multicast.typing import MultiCastItem


@dataclass
class DebugMultiCastObserver(MultiCastObserver):
    source: MultiCastObserver
    on_next_func: Callable[[Any], None]
    on_completed_func: Callable[[], None]
    on_error_func: Callable[[Exception], None]

    def __post_init__(self):
        self.has_scheduled_next = False

    def on_next(self, elem: MultiCastItem) -> None:
        try:
            elem = list(elem)

            if len(elem) == 0:
                return

            self.on_next_func(elem)
            self.source.on_next(elem)

        except Exception as exc:
            self.on_error(exc)
            return

    def on_error(self, exc: Exception) -> None:
        self.on_error_func(exc)
        self.source.on_error(exc)

    def on_completed(self) -> None:
        self.on_completed_func()
        self.source.on_completed()