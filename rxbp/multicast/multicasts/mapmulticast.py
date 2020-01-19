from typing import Callable

import rx
from rx import operators as rxop

from rxbp.multicast.multicastInfo import MultiCastInfo
from rxbp.multicast.multicastbase import MultiCastBase
from rxbp.multicast.typing import MultiCastValue


class MapMultiCast(MultiCastBase):
    def __init__(
            self,
            source: MultiCastBase,
            func: Callable[[MultiCastValue], MultiCastValue],
    ):
        self.source = source
        self.func = func

    def get_source(self, info: MultiCastInfo) -> rx.typing.Observable:
        observer = self.source.get_source(info=info).pipe(
            rxop.map(self.func),
        )

        return observer
