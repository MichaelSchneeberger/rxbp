from dataclasses import replace

from dataclass_abc import dataclass_abc

from rxbp.multicast.liftedmulticast import LiftedMultiCast
from rxbp.multicast.mixins.multicastmixin import MultiCastMixin


@dataclass_abc
class LiftedMultiCastImpl(LiftedMultiCast):
    is_hot: bool
    nested_layer: int
    underlying: MultiCastMixin

    def _copy(self, underlying: MultiCastMixin = None, **kwargs):
        if underlying is not None:
            kwargs['underlying'] = underlying

        return replace(self, **kwargs)
