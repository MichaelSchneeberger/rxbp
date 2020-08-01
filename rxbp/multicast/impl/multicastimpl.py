from dataclasses import replace

from dataclass_abc import dataclass_abc

from rxbp.multicast.mixins.multicastmixin import MultiCastMixin
from rxbp.multicast.multicast import MultiCast
from rxbp.multicast.typing import MultiCastValue


@dataclass_abc
class MultiCastImpl(MultiCast[MultiCastValue]):
    underlying: MultiCastMixin

    def _copy(self, multicast: MultiCastMixin):
        return replace(self, underlying=multicast)
