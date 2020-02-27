from typing import Callable

import rx
from rx import operators as rxop

from rxbp.multicast.multicastInfo import MultiCastInfo
from rxbp.multicast.multicastbase import MultiCastBase
from rxbp.multicast.rxextensions.liftobservable import LiftObservable
from rxbp.multicast.typing import MultiCastValue


class LiftMultiCast(MultiCastBase):
    def __init__(
            self,
            source: MultiCastBase,
            func: Callable[[MultiCastBase, MultiCastValue], MultiCastValue],
    ):
        self.source = source
        self.func = func

        self.shared_observable = None

    def get_source(self, info: MultiCastInfo) -> rx.typing.Observable:
        if self.shared_observable is None:
            class InnerLiftMultiCast(MultiCastBase):
                def __init__(self, source: rx.typing.Observable[MultiCastValue]):
                    self._source = source

                def get_source(self, info: MultiCastInfo) -> rx.typing.Observable:
                    return self._source

            def lift_func(obs: rx.typing.Observable, first: MultiCastValue):
                source = obs.pipe(
                    rxop.share(),
                )
                inner_multicast = InnerLiftMultiCast(source=source)
                multicast_val = self.func(inner_multicast, first)
                return multicast_val.get_source(info=info)

            self.shared_observable = LiftObservable(
                source=self.source.get_source(info=info),
                func=lift_func,
                scheduler=info.multicast_scheduler,
            ).pipe(
                rxop.map(lambda obs: InnerLiftMultiCast(obs)),
                rxop.share(),
            )

        return self.shared_observable
