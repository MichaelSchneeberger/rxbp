from dataclasses import replace

from dataclass_abc import dataclass_abc

from rxbp.flowable import Flowable
from rxbp.mixins.flowablebasemixin import FlowableBaseMixin
from rxbp.typing import ValueType


@dataclass_abc
class FlowableImpl(Flowable[ValueType]):
    underlying: FlowableBaseMixin

    def _copy(self, flowable: FlowableBaseMixin):
        return replace(self, underlying=flowable)
