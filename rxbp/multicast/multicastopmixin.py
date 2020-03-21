from abc import ABC, abstractmethod
from typing import Callable, Any

import rx

from rxbp.multicast.flowableop import FlowableOp
from rxbp.multicast.typing import MultiCastValue
from rxbp.typing import ValueType


class MultiCastOpMixin(ABC):
    @abstractmethod
    def debug(self, name: str = None):
        ...

    @abstractmethod
    def default_if_empty(
            self,
            lazy_val: Callable[[], Any],
    ):
        ...

    @abstractmethod
    def loop_flowables(
            self,
            func: Callable[[MultiCastValue], MultiCastValue],
            initial: ValueType,
    ):
        ...

    @abstractmethod
    def empty(self):
        ...

    @abstractmethod
    def filter(
            self,
            predicate: Callable[[MultiCastValue], bool],
    ):
        ...

    @abstractmethod
    def first(
            self,
            raise_exception: Callable[[Callable[[], None]], None],
    ):
        ...

    @abstractmethod
    def first_or_default(
            self,
            lazy_val: Callable[[], Any],
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
    def map_with_op(self, func: Callable[[MultiCastValue, FlowableOp], MultiCastValue]):
        ...

    @abstractmethod
    def observe_on(self, scheduler: rx.typing.Scheduler):
        ...

    @abstractmethod
    def collect_flowables(
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
    def join_flowables(
            self,
            *others: 'MultiCastOpMixin',
    ):
        ...
