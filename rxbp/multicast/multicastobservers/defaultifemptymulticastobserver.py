from dataclasses import dataclass
from typing import Any, Callable

from rxbp.multicast.multicastobserver import MultiCastObserver
from rxbp.multicast.typing import MultiCastItem


@dataclass
class DefaultIfEmptyMultiCastObserver(MultiCastObserver):
    source: MultiCastObserver
    lazy_val: Callable[[], Any]
    found: bool

    def on_next(self, elem: MultiCastItem) -> None:
        self.found = True
        self.source.on_next(elem)

    def on_error(self, exc: Exception) -> None:
        self.source.on_error(exc)

    def on_completed(self) -> None:
        if not self.found:
            self.source.on_next([self.lazy_val()])
        self.source.on_completed()
