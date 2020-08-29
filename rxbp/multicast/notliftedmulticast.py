from abc import ABC
from typing import Generic

from rxbp.multicast.init.initliftedmulticast import init_lifted_multicast
from rxbp.multicast.multicast import MultiCast
from rxbp.multicast.multicasts.liftmulticast import LiftMultiCast
from rxbp.multicast.typing import MultiCastElemType


class NotLiftedMultiCast(
    MultiCast[MultiCastElemType],
    Generic[MultiCastElemType],
    ABC,
):
    def lift(
            self,
    ):
        return self._copy(
            underlying=LiftMultiCast(
                source=init_lifted_multicast(
                    underlying=self,
                    nested_layer=self.nested_layer,
                ).share(),
            ),
            nested_layer=self.nested_layer + 1,
        )
