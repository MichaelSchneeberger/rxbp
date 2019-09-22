from abc import abstractmethod, ABC
from typing import Callable

from rxbp.stream.streambase import StreamBase


class StreamOperator(ABC):
    def __init__(self, func: Callable[[StreamBase], StreamBase]):
        self.func = func

    # @abstractmethod
    def __call__(self, stream: StreamBase):
        return self.func(stream)