from abc import ABC
from typing import Callable, Any, Generic

from rxbp.multicast.init.initliftedmulticast import init_lifted_multicast
from rxbp.multicast.mixins.multicastmixin import MultiCastMixin
from rxbp.multicast.multicast import MultiCast
from rxbp.multicast.multicasts.liftmulticast import LiftMultiCast
from rxbp.multicast.multicasts.sharedmulticast import SharedMultiCast
from rxbp.multicast.typing import MultiCastElemType


class NotLiftedMultiCast(
    MultiCast[MultiCastElemType],
    Generic[MultiCastElemType],
    ABC,
):
    def lift(
            self,
            func: Callable[[MultiCast], Any] = None,
    ):
        if func is None:
            func = lambda m: m

        def lifted_func(m: MultiCastMixin):
            return func(init_lifted_multicast(
                underlying=m,
                nested_layer=self.nested_layer,
            ))

        return self._copy(
            underlying=LiftMultiCast(
                source=SharedMultiCast(source=self.materialize()),
                func=lifted_func,
            ),
            nested_layer=self.nested_layer + 1,
        )

    # def share(
    #         self,
    #         func: Callable[[MultiCast], MultiCast],
    # ):
    #     def lifted_func(m: MultiCastMixin):
    #         val = func(init_lifted_multicast(
    #             underlying=m,
    #             nested_layer=self.nested_layer,
    #         ))
    #         return val
    #
    #     return self._copy(lifted_func(SharedMultiCast(source=self)))
