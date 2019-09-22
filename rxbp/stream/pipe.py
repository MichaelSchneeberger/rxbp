from typing import Callable
from functools import reduce

from rxbp.stream.streambase import StreamBase


def pipe(*operators: Callable[[StreamBase], StreamBase]) -> Callable[[StreamBase], StreamBase]:

    def compose(source: StreamBase) -> StreamBase:
        return reduce(lambda obs, op: op(obs), operators, source)
    return compose