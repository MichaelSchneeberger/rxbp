from abc import ABC
from typing import Generic, Callable, Any

from rxbp.multicast.multicast import MultiCast
from rxbp.multicast.multicasts.sharedmulticast import SharedMultiCast
from rxbp.multicast.typing import MultiCastElemType


class LiftedMultiCast(
    MultiCast[MultiCastElemType],
    Generic[MultiCastElemType],
    ABC,
):
    def share(self):
        return self._copy(SharedMultiCast(source=self))

    def lift(
            self,
            func: Callable[['MultiCast'], Any],
    ):
        raise Exception('Only non-lifted MultiCast implements the lift operator')
