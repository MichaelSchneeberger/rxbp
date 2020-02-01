from abc import abstractmethod, ABC
from typing import Callable, Any, Iterator

from rxbp.flowablebase import FlowableBase
from rxbp.scheduler import Scheduler
from rxbp.selectors.base import Base
from rxbp.typing import ValueType


class FlowableOpMixin(ABC):
    @abstractmethod
    def buffer(self, buffer_size: int) -> FlowableBase:
        ...

    @abstractmethod
    def concat(self, *sources: FlowableBase) -> FlowableBase:
        ...

    @abstractmethod
    def controlled_zip(
            self,
            right: FlowableBase,
            request_left: Callable[[Any, Any], bool] = None,
            request_right: Callable[[Any, Any], bool] = None,
            match_func: Callable[[Any, Any], bool] = None,
    ) -> FlowableBase:
        ...

    @abstractmethod
    def debug(
            self,
            name=None,
            on_next=None,
            on_subscribe=None,
            on_ack=None,
            on_raw_ack=None,
            on_ack_msg=None,
    ) -> FlowableBase:
        ...

    @abstractmethod
    def fast_filter(self, predicate: Callable[[Any], bool]) -> FlowableBase:
        ...

    @abstractmethod
    def filter(self, predicate: Callable[[Any], bool]) -> FlowableBase:
        ...

    @abstractmethod
    def filter_with_index(self, predicate: Callable[[Any, int], bool]) -> FlowableBase:
        ...

    @abstractmethod
    def first(self, raise_exception: Callable[[Callable[[], None]], None] = None) -> FlowableBase:
        ...

    @abstractmethod
    def flat_map(self, func: Callable[[Any], FlowableBase]) -> FlowableBase:
        ...

    @abstractmethod
    def map(self, func: Callable[[Any], Any]) -> FlowableBase:
        ...

    @abstractmethod
    def map_to_iterator(
            self,
            func: Callable[[ValueType], Iterator[ValueType]],
    ):
        ...

    @abstractmethod
    def match(self, *others: FlowableBase) -> FlowableBase:
        ...

    @abstractmethod
    def merge(self, *others: FlowableBase) -> FlowableBase:
        ...

    @abstractmethod
    def observe_on(self, scheduler: Scheduler) -> FlowableBase:
        ...

    @abstractmethod
    def pairwise(self) -> FlowableBase:
        ...

    @abstractmethod
    def reduce(
            self,
            func: Callable[[Any, Any], Any],
            initial: Any,
    ) -> FlowableBase:
        ...

    @abstractmethod
    def repeat_first(self) -> FlowableBase:
        ...

    @abstractmethod
    def scan(self, func: Callable[[Any, Any], Any], initial: Any) -> FlowableBase:
        ...

    def share(self) -> FlowableBase:
        raise Exception('this Flowable cannot be shared. Use multicasting to share Flowables.')

    @abstractmethod
    def _share(self) -> FlowableBase:
        ...

    @abstractmethod
    def set_base(self, val: Base) -> FlowableBase:
        ...

    @abstractmethod
    def to_list(self) -> FlowableBase:
        ...

    @abstractmethod
    def zip(self, *others: FlowableBase) -> FlowableBase:
        ...

    @abstractmethod
    def zip_with_index(self) -> FlowableBase:
        ...
