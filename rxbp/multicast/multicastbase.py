from abc import ABC, abstractmethod
from dataclasses import dataclass

from typing import Generic

import rxbp

from rxbp.flowable import Flowable
from rxbp.typing import ValueType
from rxbp.multicast.typing import MultiCastValue


class MultiCastBase(Generic[MultiCastValue], ABC):
    @dataclass
    class LiftedFlowable:
        source: Flowable

    def to_flowable(self) -> Flowable[ValueType]:
        def flat_map_func(v: MultiCastValue):
            if isinstance(v, MultiCastBase.LiftedFlowable):
                return v.source
            else:
                return v

        return self.source.pipe(
            rxbp.op.filter(lambda v: isinstance(v, MultiCastBase.LiftedFlowable) or isinstance(v, Flowable)),
            rxbp.op.first(),
            rxbp.op.subscribe_on(),
            rxbp.op.flat_map(flat_map_func),
        )

    @property
    @abstractmethod
    def source(self) -> Flowable:
        ...
