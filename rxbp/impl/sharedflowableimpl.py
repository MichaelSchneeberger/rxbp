from dataclasses import replace

from dataclass_abc import dataclass_abc

from rxbp.flowable import Flowable
from rxbp.mixins.flowablemixin import FlowableMixin
from rxbp.typing import ValueType


@dataclass_abc
class SharedFlowableImpl(Flowable[ValueType]):
    underlying: FlowableMixin

    def _copy(self, underlying: FlowableMixin, is_shared: bool = None, *args, **kwargs):
        return replace(self, underlying=underlying, *args, **kwargs)
