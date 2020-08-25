import functools
from abc import ABC
from typing import Generic, Callable, Any

from rxbp.multicast.multicast import MultiCast
from rxbp.multicast.multicastoperator import MultiCastOperator
from rxbp.multicast.multicasts.sharedmulticast import SharedMultiCast
from rxbp.multicast.typing import MultiCastElemType


class LiftedMultiCast(
    MultiCast[MultiCastElemType],
    Generic[MultiCastElemType],
    ABC,
):
    def share(self):
        return self._copy(SharedMultiCast(source=self.materialize()))

    def pipe(self, *operators: MultiCastOperator) -> 'LiftedMultiCast':
        return functools.reduce(lambda acc, op: op(acc), operators, self).share()