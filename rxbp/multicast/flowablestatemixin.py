from abc import ABC, abstractmethod
from typing import Any, Dict, Generic

from rxbp.flowable import Flowable
from rxbp.typing import ValueType


class FlowableStateMixin(Generic[ValueType], ABC):
    @abstractmethod
    def get_flowable_state(self) -> Dict[Any, Flowable[ValueType]]:
        ...

    @abstractmethod
    def set_flowable_state(self, val: Dict[Any, Flowable[ValueType]]) -> Any:
        ...
