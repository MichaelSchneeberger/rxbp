from abc import abstractmethod, ABC
from dataclasses import dataclass
from typing import Callable

from rxbp.multicast.multicastbase import MultiCastBase


@dataclass
class MultiCastOperator(ABC):
    func: Callable[[MultiCastBase], MultiCastBase]

    def __call__(self, stream: MultiCastBase) -> MultiCastBase:
        return self.func(stream)
