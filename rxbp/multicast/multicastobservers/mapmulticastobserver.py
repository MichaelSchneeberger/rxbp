from dataclasses import dataclass
from typing import Callable

from rxbp.multicast.multicastobserver import MultiCastObserver
from rxbp.multicast.typing import MultiCastItem


@dataclass
class MapMultiCastObserver(MultiCastObserver):
    source: MultiCastObserver
    func: Callable[[MultiCastItem], MultiCastItem]

    def on_next(self, item: MultiCastItem) -> None:
        try:
            def map_gen():
                for v in item:
                    yield self.func(v)

            next = map_gen()
        except Exception as exc:
            self.source.on_error(exc)
        else:
            self.source.on_next(next)

    def on_error(self, exc: Exception) -> None:
        self.source.on_error(exc)

    def on_completed(self) -> None:
        self.source.on_completed()