from typing import Callable
from functools import reduce

from rxbp.flowablebase import FlowableBase
from rxbp.selectors.bases import Base


def pipe(*operators: Callable[[Base], Base]) -> Callable[[Base], Base]:

    def compose(source: FlowableBase) -> Base:
        return reduce(lambda obs, op: op(obs), operators, source)
    return compose
