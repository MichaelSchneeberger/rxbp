from typing import Callable

import rx
from rx import operators as rxop

from rxbp.multicast.multicastInfo import MultiCastInfo
from rxbp.multicast.mixins.multicastmixin import MultiCastMixin
from rxbp.multicast.typing import MultiCastValue


class FlatMapMultiCast(MultiCastMixin):
    def __init__(
            self,
            source: MultiCastMixin,
            func: Callable[[MultiCastValue], MultiCastMixin[MultiCastValue]],
    ):
        self.source = source
        self.func = func

    def get_source(self, info: MultiCastInfo) -> rx.typing.Observable:
        def check_return_value_of_func(value):
            multi_cast = self.func(value)

            if not isinstance(multi_cast, MultiCastMixin):
                raise Exception(f'"{self.func}" should return a "MultiCast", but returned "{multi_cast}"')

            return multi_cast.get_source(info=info)

        return self.source.get_source(info=info).pipe(
            rxop.flat_map(check_return_value_of_func),
        )
