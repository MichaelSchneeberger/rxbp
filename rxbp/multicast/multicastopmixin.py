from abc import ABC, abstractmethod
from typing import Callable

from rxbp.multicast.typing import MultiCastValue
from rxbp.typing import ValueType


class MultiCastOpMixin(ABC):
    @abstractmethod
    def debug(self, name: str = None):
        ...

    @abstractmethod
    def loop_flowable(
            self,
            func: Callable[[MultiCastValue], MultiCastValue],
            initial: ValueType,
    ):
        ...

    @abstractmethod
    def empty(self):
        ...

    # @abstractmethod
    # def share_flowable(
    #         self,
    #         func: Callable[[MultiCastValue], Union[Flowable, List, Dict, FlowableStateMixin]],
    # ):
    #     ...

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

    # @abstractmethod
    # def map_with_context(self, func: Callable[[MultiCastValue, MultiCastContext], MultiCastValue]):
    #     ...

    @abstractmethod
    def reduce_flowable(
            self,
            maintain_order: bool = None,
    ):
        ...

    @abstractmethod
    def _share(self):
        ...

    def share(self) -> 'MultiCastOpMixin':
        raise Exception('this MultiCast cannot be shared. Use "lift" operator to share this MultiCast.')

    @abstractmethod
    def connect_flowable(
            self,
            *others: 'MultiCastOpMixin',
    ):
        ...
