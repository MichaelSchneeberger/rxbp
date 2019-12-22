from abc import abstractmethod, ABC
from typing import Callable, Any

from rxbp.scheduler import Scheduler
from rxbp.selectors.bases import Base


class FlowableOpMixin(ABC):
    @abstractmethod
    def buffer(self, buffer_size: int) -> 'FlowableOpMixin':
        ...

    @abstractmethod
    def concat(self, *sources: 'FlowableOpMixin') -> 'FlowableOpMixin':
        ...

    @abstractmethod
    def controlled_zip(
            self,
            right: 'FlowableOpMixin',
            request_left: Callable[[Any, Any], bool] = None,
            request_right: Callable[[Any, Any], bool] = None,
            match_func: Callable[[Any, Any], bool] = None,
    ) -> 'FlowableOpMixin':
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
    ) -> 'FlowableOpMixin':
        ...

    @abstractmethod
    def fast_filter(self, predicate: Callable[[Any], bool]) -> 'FlowableOpMixin':
        ...

    @abstractmethod
    def filter(self, predicate: Callable[[Any], bool]) -> 'FlowableOpMixin':
        ...

    @abstractmethod
    def filter_with_index(self, predicate: Callable[[Any, int], bool]) -> 'FlowableOpMixin':
        ...

    @abstractmethod
    def first(self, raise_exception: Callable[[Callable[[], None]], None] = None) -> 'FlowableOpMixin':
        ...

    @abstractmethod
    def flat_map(self, selector: Callable[[Any], 'FlowableOpMixin']) -> 'FlowableOpMixin':
        ...

    @abstractmethod
    def map(self, selector: Callable[[Any], Any]) -> 'FlowableOpMixin':
        ...

    @abstractmethod
    def match(self, *others: 'FlowableOpMixin') -> 'FlowableOpMixin':
        ...

    @abstractmethod
    def merge(self, *others: 'FlowableOpMixin') -> 'FlowableOpMixin':
        ...

    @abstractmethod
    def observe_on(self, scheduler: Scheduler) -> 'FlowableOpMixin':
        ...

    @abstractmethod
    def pairwise(self) -> 'FlowableOpMixin':
        ...

    @abstractmethod
    def repeat_first(self) -> 'FlowableOpMixin':
        ...

    @abstractmethod
    def scan(self, func: Callable[[Any, Any], Any], initial: Any) -> 'FlowableOpMixin':
        ...

    @abstractmethod
    def set_base(self, val: Base) -> 'FlowableOpMixin':
        ...

    @abstractmethod
    def to_list(self) -> 'FlowableOpMixin':
        ...

    @abstractmethod
    def zip(self, *others: 'FlowableOpMixin') -> 'FlowableOpMixin':
        ...

    @abstractmethod
    def zip_with_index(self) -> 'FlowableOpMixin':
        ...
