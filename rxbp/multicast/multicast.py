import functools
from abc import ABC, abstractmethod
from typing import Generic

from rxbp.multicast.mixins.liftedmulticastmixin import LiftedMultiCastMixin
from rxbp.multicast.mixins.multicastmixin import MultiCastMixin
from rxbp.multicast.mixins.multicastopmixin import MultiCastOpMixin
from rxbp.multicast.mixins.toflowablemixin import ToFlowableMixin
from rxbp.multicast.multicastoperator import MultiCastOperator
from rxbp.multicast.typing import MultiCastElemType


class MultiCast(
    LiftedMultiCastMixin,
    MultiCastOpMixin,
    ToFlowableMixin,
    MultiCastMixin,
    Generic[MultiCastElemType],
    ABC,
):
    """
    A `MultiCast` represents a collection of *Flowable* and can
    be though of as `Flowable[T[Flowable]]`
    """

    @abstractmethod
    def _copy(self, *args, **kwargs) -> 'MultiCast':
        ...

    def pipe(self, *operators: MultiCastOperator) -> 'MultiCast':
        return functools.reduce(lambda acc, op: op(acc), operators, self)
