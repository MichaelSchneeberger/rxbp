import rx
from rx import operators as rxop

from rxbp.multicast.multicastInfo import MultiCastInfo
from rxbp.multicast.multicastbase import MultiCastBase
from rxbp.multicast.typing import MultiCastValue


class ObserveOnMultiCast(MultiCastBase):
    def __init__(
            self,
            source: MultiCastBase,
            scheduler: rx.typing.Scheduler,
    ):
        self.source = source
        self.scheduler = scheduler

    def get_source(
            self,
            info: MultiCastInfo,
    ) -> rx.typing.Observable[MultiCastValue]:
        source = self.source.get_source(info=info)
        return source.pipe(
            rxop.observe_on(self.scheduler),
        )
