from abc import ABC, abstractmethod
from typing import List, Callable, Union, Dict

from rxbp.flowable import Flowable
from rxbp.multicast.flowablestatemixin import FlowableStateMixin
from rxbp.multicast.typing import MultiCastValue
from rxbp.multicastcontext import MultiCastContext
from rxbp.typing import ValueType


class MultiCastOpMixin(ABC):
    @abstractmethod
    def debug(self, name: str = None):
        ...

    @abstractmethod
    def defer(
            self,
            func: Callable[[MultiCastValue], MultiCastValue],
            initial: ValueType,
    ):
        ...

    @abstractmethod
    def empty(self):
        ...

    @abstractmethod
    def share_flowable(
            self,
            func: Callable[[MultiCastValue], Union[Flowable, List, Dict, FlowableStateMixin]],
    ):
        ...

    @abstractmethod
    def filter(
            self,
            func: Callable[[MultiCastValue], bool],
    ):
        ...

    @abstractmethod
    def flat_map(
            self,
            func: Callable[[MultiCastValue], 'MultiCastOpMixin[MultiCastValue]'],
    ):
        ...

    @abstractmethod
    def lift(
            self,
            func: Callable[['MultiCastOpMixin'], MultiCastValue],
    ):
        ...

    @abstractmethod
    def merge(self, *others: 'MultiCastOpMixin'):
        ...

    @abstractmethod
    def map(self, func: Callable[[MultiCastValue], MultiCastValue]):
        ...

    @abstractmethod
    def map_with_context(self, func: Callable[[MultiCastValue, MultiCastContext], MultiCastValue]):
        ...

    @abstractmethod
    def reduce(
            self,
            maintain_order: bool = None,
    ):
        ...

    @abstractmethod
    def share(self):
        ...

    @abstractmethod
    def zip(
            self,
            *others: 'MultiCastOpMixin',
    ):
        ...
