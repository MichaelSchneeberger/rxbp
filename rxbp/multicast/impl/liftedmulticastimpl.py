import dataclasses

from dataclass_abc import dataclass_abc

from rxbp.multicast.liftedmulticast import LiftedMultiCast
from rxbp.multicast.mixins.multicastmixin import MultiCastMixin
from rxbp.multicast.typing import MultiCastElemType


@dataclass_abc
class LiftedMultiCastImpl(LiftedMultiCast[MultiCastElemType]):
    underlying: MultiCastMixin
    lift_index: int

    def _copy(self, **kwargs):
        return dataclasses.replace(self, **kwargs)
