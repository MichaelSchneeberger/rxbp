from dataclasses import dataclass
from typing import Callable, Any, Iterator

from rxbp.acknowledgement.stopack import stop_ack
from rxbp.observer import Observer
from rxbp.typing import ElementType


@dataclass
class MapToIteratorObserver(Observer):
    source: Observer
    func: Callable[[Any], Iterator[Any]]

    def on_next(self, elem: ElementType):
        def map_gen():
            for v in elem:
                yield from self.func(v)

        try:
            buffer = list(map_gen())
        except Exception as exc:
            self.source.on_error(exc)
            return stop_ack

        return self.source.on_next(buffer)

    def on_error(self, exc):
        return self.source.on_error(exc)

    def on_completed(self):
        return self.source.on_completed()