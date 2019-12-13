from abc import ABC
from dataclasses import dataclass
from typing import Callable

from rxbp.multicast.multicastopmixin import MultiCastOpMixin


@dataclass
class MultiCastOperator(ABC):
    func: Callable[[MultiCastOpMixin], MultiCastOpMixin]

    def __call__(self, stream: MultiCastOpMixin):
        return self.func(stream)
