import functools
from abc import ABC
from typing import Generic

from rxbp.multicast.mixins.ishotmulticastmixin import IsHotMultiCastMixin
from rxbp.multicast.mixins.multicastmixin import MultiCastMixin
from rxbp.multicast.mixins.toflowablemixin import ToFlowableMixin
from rxbp.multicast.multicastoperator import MultiCastOperator
from rxbp.multicast.multicastopmixin import MultiCastOpMixin
from rxbp.multicast.typing import MultiCastElemType


class MultiCast(
    IsHotMultiCastMixin,
    MultiCastOpMixin,
    ToFlowableMixin,
    MultiCastMixin,
    # Generic[MultiCastElemType],
    ABC,
):
    """
    A `MultiCast` represents a collection of *Flowable* and can
    be though of as `Flowable[T[Flowable]]`
    """

    def pipe(self, *operators: MultiCastOperator) -> 'MultiCastMixin':
        return functools.reduce(lambda acc, op: op(acc), operators, self)
