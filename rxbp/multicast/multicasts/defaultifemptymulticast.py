from typing import Any

import rx
from rx import operators as rxop

from rxbp.multicast.multicastInfo import MultiCastInfo
from rxbp.multicast.multicastbase import MultiCastBase
from rxbp.multicast.typing import MultiCastValue


class DefaultIfEmptyMultiCast(MultiCastBase):
    def __init__(
            self,
            source: MultiCastBase,
            val: Any,
    ):
        self.source = source
        self.val = val

    def get_source(self, info: MultiCastInfo) -> rx.typing.Observable[MultiCastValue]:
        return self.source.get_source(info=info).pipe(
            rxop.default_if_empty(self.val),
        )
