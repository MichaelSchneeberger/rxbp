from typing import Generic

import rx
from rxbp.flowable import Flowable
from rxbp.multicast.multicastInfo import MultiCastInfo

from rxbp.multicast.pipe import pipe
from rxbp.multicast.multicastbase import MultiCastBase
from rxbp.multicast.multicastoperator import MultiCastOperator
from rxbp.multicast.typing import MultiCastValue


class MultiCast(MultiCastBase, Generic[MultiCastValue]):
    """ A `MultiCast` represents a collection of *Flowable* and can
     be though of as `Flowable[T[Flowable]]` where T is defined by the user.


    """

    def __init__(self, underlying: MultiCastBase):
        self.underlying = underlying

    def pipe(self, *operators: MultiCastOperator):
        return pipe(*operators)(self)

    def get_source(self, info: MultiCastInfo) -> rx.typing.Observable[MultiCastValue]:
        return self.underlying.get_source(info=info)
