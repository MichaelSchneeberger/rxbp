from abc import abstractmethod, ABC
from dataclasses import dataclass
from typing import Callable

from rxbp.multicast.multicastbase import MultiCastBase


@dataclass
class MultiCastOperator(ABC):

    @dataclass
    class ReturnValue:
        multi_cast: MultiCastBase
        n_lifts: int = 0

    # def __init__(
    #         self, func: Callable[[MultiCastBase], ReturnValue]):
    #     self.func = func

    func: Callable[[MultiCastBase], ReturnValue]

    def __call__(self, stream: MultiCastBase) -> ReturnValue:
        return self.func(stream)
