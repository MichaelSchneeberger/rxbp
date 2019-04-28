from typing import Callable
from functools import reduce

from rxbp.flowablebase import FlowableBase
from .observable import Observable


def pipe(*operators: Callable[[FlowableBase], FlowableBase]) -> Callable[[FlowableBase], FlowableBase]:

    def compose(source: FlowableBase) -> FlowableBase:
        return reduce(lambda obs, op: op(obs), operators, source)
    return compose
