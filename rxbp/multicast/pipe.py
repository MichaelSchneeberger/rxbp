from typing import Callable, Tuple
from functools import reduce

from rxbp.multicast.multicastbase import MultiCastBase
from rxbp.multicast.multicastoperator import MultiCastOperator


def pipe(*operators: MultiCastOperator) -> MultiCastOperator: #Callable[[MultiCastBase], MultiCastBase]) -> Callable[[MultiCastBase], MultiCastBase]:

    def compose(source: MultiCastBase) -> MultiCastBase:
        def func(val: MultiCastOperator.ReturnValue, op: MultiCastOperator):
            next_val = op(val.multi_cast)
            return MultiCastOperator.ReturnValue(multi_cast=next_val.multi_cast, n_lifts=val.n_lifts + next_val.n_lifts)

        return reduce(
            func,
            operators,
            MultiCastOperator.ReturnValue(multi_cast=source, n_lifts=0),
        )
    return compose