from dataclasses import replace

from dataclass_abc import dataclass_abc

from rxbp.mixins.flowablemixin import FlowableMixin
from rxbp.sharedflowable import SharedFlowable
from rxbp.typing import ValueType


@dataclass_abc
class SharedFlowableImpl(SharedFlowable[ValueType]):
    underlying: FlowableMixin

    def _copy(
            self,
            is_shared: bool = None,
            **kwargs,
    ):
        return replace(self, **kwargs)
