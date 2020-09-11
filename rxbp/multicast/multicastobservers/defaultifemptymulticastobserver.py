import types
from dataclasses import dataclass
from typing import Any, Callable

from rxbp.multicast.multicastobserver import MultiCastObserver
from rxbp.multicast.typing import MultiCastItem


@dataclass
class DefaultIfEmptyMultiCastObserver(MultiCastObserver):
    source: MultiCastObserver
    lazy_val: Callable[[], Any]

    def __post_init__(self):
        self.found = False

    def on_next(self, item: MultiCastItem) -> None:
        if isinstance(item, list):
            materialized = item
        else:
            materialized = list(item)

        if len(materialized) == 0:
            return

        self.found = True

        self.on_next = types.MethodType(lambda _, v: self.source.on_next(v), self)  # type: ignore

        self.source.on_next(materialized)

    def on_error(self, exc: Exception) -> None:
        self.source.on_error(exc)

    def on_completed(self) -> None:
        if not self.found:
            self.source.on_next([self.lazy_val()])
        self.source.on_completed()
