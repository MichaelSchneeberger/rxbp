from typing import Callable

import rx
from rx import operators as rxop

from rxbp.multicast.multicastInfo import MultiCastInfo
from rxbp.multicast.multicastbase import MultiCastBase
from rxbp.multicast.typing import MultiCastValue


class FilterMultiCast(MultiCastBase):
    def __init__(
            self,
            source: MultiCastBase,
            predicate: Callable[[MultiCastValue], bool],
    ):
        self.source = source
        self.predicate = predicate

    def get_source(self, info: MultiCastInfo) -> rx.typing.Observable:
        source = self.source.get_source(info=info).pipe(
            rxop.filter(self.predicate)
        )
        return source
