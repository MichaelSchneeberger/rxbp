from typing import Callable, Tuple
from functools import reduce

from rxbp.multicast.multicastbase import MultiCastBase
from rxbp.multicast.multicastoperator import MultiCastOperator


def pipe(*operators: Callable[[MultiCastBase], MultiCastBase]) -> Callable[[MultiCastBase], MultiCastBase]:

    def compose(source: MultiCastBase) -> MultiCastBase:
        def func(val: MultiCastBase, op: MultiCastOperator):
            return op(val)

        return reduce(func, operators, source,)
    return compose