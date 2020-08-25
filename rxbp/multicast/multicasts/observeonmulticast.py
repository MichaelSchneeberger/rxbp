import rx
from rx import operators as rxop

from rxbp.multicast.mixins.multicastmixin import MultiCastMixin
from rxbp.multicast.multicastsubscriber import MultiCastSubscriber
from rxbp.multicast.typing import MultiCastItem


class ObserveOnMultiCast(MultiCastMixin):
    def __init__(
            self,
            source: MultiCastMixin,
            scheduler: rx.typing.Scheduler,
    ):
        self.source = source
        self.scheduler = scheduler

    def unsafe_subscribe(
            self,
            info: MultiCastSubscriber,
    ) -> rx.typing.Observable[MultiCastItem]:
        source = self.source.get_source(info=info)
        return source.pipe(
            rxop.observe_on(self.scheduler),
        )
