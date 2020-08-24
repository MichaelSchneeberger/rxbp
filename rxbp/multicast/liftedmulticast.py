from abc import ABC

from rxbp.multicast.multicast import MultiCast
from rxbp.multicast.multicasts.sharedmulticast import SharedMultiCast


class LiftedMultiCast(
    MultiCast,
    ABC,
):
    def share(self):
        return self._copy(SharedMultiCast(source=self))
