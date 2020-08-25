import dataclasses

from dataclass_abc import dataclass_abc

from rxbp.multicast.mixins.multicastmixin import MultiCastMixin
from rxbp.multicast.notliftedmulticast import NotLiftedMultiCast
from rxbp.multicast.typing import MultiCastElemType


@dataclass_abc
class NotLiftedMultiCastImpl(NotLiftedMultiCast[MultiCastElemType]):
    is_hot_on_subscribe: bool
    nested_layer: int
    underlying: MultiCastMixin

    def _copy(self, underlying: MultiCastMixin = None, **kwargs):
        if underlying is not None:
            kwargs['underlying'] = underlying

        return dataclasses.replace(self, **kwargs)
