import functools
from abc import ABC
from typing import Generic

from rxbp.multicast.multicast import MultiCast
from rxbp.multicast.multicastoperator import MultiCastOperator
from rxbp.multicast.typing import MultiCastElemType
from rxbp.utils.getstacklines import get_stack_lines


class LiftedMultiCast(
    MultiCast[MultiCastElemType],
    Generic[MultiCastElemType],
    ABC,
):
    def share(self):
        stack = get_stack_lines()

        return self._share(stack=stack)

    def pipe(self, *operators: MultiCastOperator) -> 'LiftedMultiCast':
        stack = get_stack_lines()

        multicast = functools.reduce(lambda acc, op: op(acc), operators, self)

        return multicast._share(stack=stack)
