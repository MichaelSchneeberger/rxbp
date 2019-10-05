from typing import Generic

from rxbp.flowable import Flowable

from rxbp.multicast.pipe import pipe
from rxbp.multicast.multicastbase import MultiCastBase
from rxbp.multicast.multicastoperator import MultiCastOperator
from rxbp.multicast.typing import MultiCastValue


class MultiCast(MultiCastBase, Generic[MultiCastValue]):
    def __init__(self, underlying: MultiCastBase):
        self.underlying = underlying

    def pipe(self, *operators: MultiCastOperator):
        return pipe(*operators)(self)

    def get_source(self, info: MultiCastBase.MultiCastInfo) -> Flowable:
        return self.underlying.get_source(info=info)
