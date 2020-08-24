import dataclasses

from dataclass_abc import dataclass_abc

from rxbp.multicast.mixins.multicastmixin import MultiCastMixin
from rxbp.multicast.notliftedmulticast import NotLiftedMultiCast


@dataclass_abc
class MultiCastImpl(NotLiftedMultiCast):
    is_hot: bool
    nested_layer: int
    underlying: MultiCastMixin

    def _copy(self, underlying: MultiCastMixin = None, **kwargs):
        if underlying is not None:
            kwargs['underlying'] = underlying

        return dataclasses.replace(self, **kwargs)
