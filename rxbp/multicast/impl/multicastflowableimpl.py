from dataclasses import replace

from dataclass_abc import dataclass_abc

from rxbp.flowable import Flowable
from rxbp.mixins.flowablebasemixin import FlowableBaseMixin
from rxbp.multicast.multicastflowable import MultiCastFlowable
from rxbp.typing import ValueType


@dataclass_abc
class MultiCastFlowableImpl(MultiCastFlowable[ValueType]):
    underlying: FlowableBaseMixin

    def _copy(self, flowable: FlowableBaseMixin):
        return replace(self, underlying=flowable)
