from typing import Callable
from functools import reduce

from rxbp.flowablebase import FlowableBase
from rxbp.selectors.bases import Base


def pipe(*operators: Callable[[FlowableBase], FlowableBase]):

    def compose(source: FlowableBase) -> FlowableBase:
        return reduce(lambda obs, op: op(obs), operators, source)
    return compose
