from dataclasses import replace

from dataclass_abc import dataclass_abc

from rxbp.multicast.liftedmulticast import LiftedMultiCast
from rxbp.multicast.mixins.multicastmixin import MultiCastMixin
from rxbp.multicast.typing import MultiCastElemType


@dataclass_abc
class LiftedMultiCastImpl(LiftedMultiCast[MultiCastElemType]):
    is_hot_on_subscribe: bool
    nested_layer: int
    underlying: MultiCastMixin

    def _copy(self, underlying: MultiCastMixin = None, **kwargs):
        if underlying is not None:
            kwargs['underlying'] = underlying

        return replace(self, **kwargs)
