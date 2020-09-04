from dataclasses import dataclass
from typing import Callable

from rxbp.multicast.multicastobserver import MultiCastObserver
from rxbp.multicast.typing import MultiCastItem


@dataclass
class FilterMultiCastObserver(MultiCastObserver):
    source: MultiCastObserver
    predicate: Callable[[MultiCastItem], bool]

    def on_next(self, item: MultiCastItem) -> None:
        def gen_filtered_iterable():
            for e in item:
                if self.predicate(e):
                    yield e

        return self.source.on_next(gen_filtered_iterable())

    def on_error(self, exc: Exception) -> None:
        self.source.on_error(exc)

    def on_completed(self) -> None:
        self.source.on_completed()
