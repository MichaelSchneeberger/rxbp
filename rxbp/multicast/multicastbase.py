from abc import ABC, abstractmethod

from typing import Callable, Generic

import rxbp

from rxbp.flowable import Flowable
from rxbp.multicast.typing import MultiCastValue
from rxbp.typing import ValueType


class MultiCastBase(Generic[MultiCastValue], ABC):
    def to_flowable(
            self,
            func: Callable[[MultiCastValue], Flowable[ValueType]] = None,
    ) -> Flowable[ValueType]:
        if func is None:
            func = lambda v: v

        return self.source.pipe(
            rxbp.op.flat_map(func),
        )

    @property
    @abstractmethod
    def source(self) -> Flowable:
        ...
