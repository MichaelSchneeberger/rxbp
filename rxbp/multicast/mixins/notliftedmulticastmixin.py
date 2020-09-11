from abc import ABC

from rxbp.flowable import Flowable
from rxbp.init.initflowable import init_flowable
from rxbp.multicast.flowables.frommulticastflowable import FromMultiCastFlowable
from rxbp.multicast.mixins.multicastmixin import MultiCastMixin
from rxbp.typing import ValueType


class NotLiftedMultiCastMixin(MultiCastMixin, ABC):
    def to_flowable(self) -> Flowable[ValueType]:
        return init_flowable(FromMultiCastFlowable(
            source=self,
        ))
