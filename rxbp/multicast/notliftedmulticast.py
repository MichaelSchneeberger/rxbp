from abc import ABC
from typing import Generic

from rxbp.multicast.init.initliftedmulticast import init_lifted_multicast
from rxbp.multicast.mixins.notliftedmulticastmixin import NotLiftedMultiCastMixin
from rxbp.multicast.multicast import MultiCast
from rxbp.multicast.multicasts.fromiterablemulticast import FromIterableMultiCast
from rxbp.multicast.typing import MultiCastElemType


class NotLiftedMultiCast(
    NotLiftedMultiCastMixin,
    MultiCast[MultiCastElemType],
    Generic[MultiCastElemType],
    ABC,
):
    def lift(
            self,
    ):
        return self._copy(
            underlying=FromIterableMultiCast(
                values=[init_lifted_multicast(
                    underlying=self,
                    lift_index=self.lift_index,
                ).share()],
                scheduler_index=self.lift_index + 1,
            ),
            lift_index=self.lift_index + 1,
        )
