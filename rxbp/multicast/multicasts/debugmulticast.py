import rx

from rxbp.multicast.mixins.multicastmixin import MultiCastMixin
from rxbp.multicast.multicastInfo import MultiCastInfo
from rxbp.multicast.rxextensions.rxdebugop import rx_debug_op


# todo: assert subscribe_scheduler is active
class DebugMultiCast(MultiCastMixin):
    def __init__(
            self,
            source: MultiCastMixin,
            name: str = None,
    ):
        self.source = source
        self.name = name

    def get_source(self, info: MultiCastInfo) -> rx.typing.Observable:
        print(f'{self.name}.get_source({info})')

        return self.source.get_source(info=info).pipe(
            rx_debug_op(self.name),
        )
