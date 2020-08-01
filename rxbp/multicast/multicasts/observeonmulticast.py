import rx
from rx import operators as rxop

from rxbp.multicast.multicastInfo import MultiCastInfo
from rxbp.multicast.mixins.multicastmixin import MultiCastMixin
from rxbp.multicast.typing import MultiCastValue


class ObserveOnMultiCast(MultiCastMixin):
    def __init__(
            self,
            source: MultiCastMixin,
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
