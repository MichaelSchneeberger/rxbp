from dataclasses import replace

from dataclass_abc import dataclass_abc

from rxbp.indexed.indexedflowable import IndexedFlowable
from rxbp.indexed.mixins.indexedflowablemixin import IndexedFlowableMixin
from rxbp.typing import ValueType


@dataclass_abc
class IndexedFlowableImpl(IndexedFlowable[ValueType]):
    underlying: IndexedFlowableMixin

    def _copy(self, flowable: IndexedFlowableMixin):
        return replace(self, underlying=flowable)
