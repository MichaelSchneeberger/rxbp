from typing import Generic

from rxbp.multicast.multicast import MultiCast
from rxbp.multicast.typing import MultiCastValue


class LiftedMultiCast(MultiCast[MultiCastValue], Generic[MultiCastValue]):
    def share(self):
        return self._share()
