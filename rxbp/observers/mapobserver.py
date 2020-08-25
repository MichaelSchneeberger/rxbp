from typing import Callable

from dataclass_abc import dataclass_abc

from rxbp.observer import Observer
from rxbp.typing import ElementType, ValueType


@dataclass_abc
class MapObserver(Observer): #OnErrorMixin, OnCompletedMixin, Observer):
    source: Observer
    func: Callable[[ValueType], ValueType]

    def on_next(self, elem: ElementType):
        # `map` does not consume elements from the iterator/list,
        # therefore it is not its responsibility to catch an exception
        def map_gen():
            for v in elem:
                yield self.func(v)

        return self.source.on_next(map_gen())

    def on_error(self, exc):
        return self.source.on_error(exc)

    def on_completed(self):
        return self.source.on_completed()