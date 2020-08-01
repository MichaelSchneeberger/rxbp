from typing import Callable

import rx
from rx import operators as rxop

from rxbp.multicast.multicastInfo import MultiCastInfo
from rxbp.multicast.mixins.multicastmixin import MultiCastMixin
from rxbp.multicast.typing import MultiCastValue


class FilterMultiCast(MultiCastMixin):
    def __init__(
            self,
            source: MultiCastMixin,
            predicate: Callable[[MultiCastValue], bool],
    ):
        self.source = source
        self.predicate = predicate

    def get_source(self, info: MultiCastInfo) -> rx.typing.Observable:
        source = self.source.get_source(info=info).pipe(
            rxop.filter(self.predicate)
        )
        return source
