import rx

from rxbp.multicast.multicastInfo import MultiCastInfo
from rxbp.multicast.multicastbase import MultiCastBase
from rxbp.multicast.rxextensions.debug_ import debug as rx_debug


class DebugMultiCast(MultiCastBase):
    def __init__(
            self,
            source: MultiCastBase,
            name: str,
    ):
        self.source = source
        self.name = name

    def get_source(self, info: MultiCastInfo) -> rx.typing.Observable:
        print(f'{self.name}.get_source({info})')

        return self.source.get_source(info=info).pipe(
            rx_debug(self.name),
        )
