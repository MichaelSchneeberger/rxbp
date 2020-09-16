from dataclasses import replace

from dataclass_abc import dataclass_abc

from rxbp.indexed.indexedflowable import IndexedFlowable
from rxbp.indexed.indexedsharedflowable import IndexedSharedFlowable
from rxbp.indexed.mixins.indexedflowablemixin import IndexedFlowableMixin
from rxbp.typing import ValueType


@dataclass_abc
class IndexedSharedFlowableImpl(IndexedSharedFlowable[ValueType]):
    underlying: IndexedFlowableMixin

    def _copy(
            self,
            is_shared: bool = None,
            **kwargs,
    ):
        return replace(self, **kwargs)
