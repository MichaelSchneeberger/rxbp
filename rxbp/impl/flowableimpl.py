from dataclasses import replace

from dataclass_abc import dataclass_abc

from rxbp.flowable import Flowable
from rxbp.mixins.flowablemixin import FlowableMixin
from rxbp.typing import ValueType


@dataclass_abc
class FlowableImpl(Flowable[ValueType]):
    underlying: FlowableMixin

    def _copy(self, flowable: FlowableMixin):
        return replace(self, underlying=flowable)
