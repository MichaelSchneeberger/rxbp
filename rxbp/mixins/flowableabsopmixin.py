from abc import abstractmethod, ABC
from traceback import FrameSummary
from typing import Callable, Any, Iterator, List

from rxbp.acknowledgement.ack import Ack
from rxbp.mixins.flowablemixin import FlowableMixin
from rxbp.observerinfo import ObserverInfo
from rxbp.scheduler import Scheduler
from rxbp.typing import ValueType


class FlowableAbsOpMixin(ABC):
    @abstractmethod
    def buffer(self, buffer_size: int = None) -> FlowableMixin:
        """
        Buffer the element emitted by the source without back-pressure until the buffer is full.
        """

        ...

    @abstractmethod
    def concat(self, *sources: FlowableMixin) -> FlowableMixin:
        """
        Concatentates Flowables sequences together by back-pressuring the tail Flowables until
        the current Flowable has completed.

        :param sources: other Flowables that get concatenate to this Flowable.
        """

        ...

    @abstractmethod
    def controlled_zip(
            self,
            right: FlowableMixin,
            stack: List[FrameSummary],
            request_left: Callable[[Any, Any], bool] = None,
            request_right: Callable[[Any, Any], bool] = None,
            match_func: Callable[[Any, Any], bool] = None,
    ) -> FlowableMixin:
        """
        Creates a new Flowable from two Flowables by combining their elements in pairs. Which
        element gets paired with an element from the other Flowable is determined by two functions
        called `request_left` and `request_right`.

        :param right: other Flowable
        :param request_left: a function that returns True, if a new element from the left \
        Flowable is requested to build the the next pair
        :param request_right: a function that returns True, if a new element from the left \
        Flowable is requested to build the the next pair
        :param match_func: a filter function that returns True, if the current pair is sent \
        downstream
        :return: zipped Flowable
        """

        ...

    @abstractmethod
    def debug(
            self,
            name: str,
            on_next: Callable[[Any], Ack],
            on_completed: Callable[[], None],
            on_error: Callable[[Exception], None],
            on_sync_ack: Callable[[Ack], None],
            on_async_ack: Callable[[Ack], None],
            on_subscribe: Callable[[ObserverInfo], None],
            on_raw_ack: Callable[[Ack], None],
            stack: List[FrameSummary],
    ) -> FlowableMixin:
        """ Print debug messages to the console when providing the `name` argument

        :on_next: customize the on next debug console print
        """

        ...

    @abstractmethod
    def default_if_empty(self, lazy_val: Callable[[], Any]):
        """
        Only the elements of the source or a default value if the source is an empty sequence

        :param lazy_val: a function that returns the default value
        """

        ...

    @abstractmethod
    def do_action(
            self,
            on_next: Callable[[Any], None] = None,
            on_completed: Callable[[], None] = None,
            on_error: Callable[[Exception], None] = None,
            on_disposed: Callable[[], None] = None,
    ):
        ...

    # @abstractmethod
    # def execute_on(self, scheduler: Scheduler):
    #     """
    #     Inject new scheduler that is used to subscribe the Flowable.
    #     """
    #
    #     ...

    @abstractmethod
    def filter(
            self,
            predicate: Callable[[Any], bool],
            stack: List[FrameSummary],
    ) -> FlowableMixin:
        """ Only emit those elements for which the given predicate holds.

        :param predicate: a function that returns True, if the current element passes the filter
        :return: filtered Flowable
        """

        ...

    @abstractmethod
    def first(self, stack: List[FrameSummary]) -> FlowableMixin:
        """
        Emit the first element only and stop the Flowable sequence thereafter.
        """

        ...

    # @abstractmethod
    # def first_or_default(self, lazy_val: Callable[[], Any]) -> FlowableTemplateMixin:
    #     """
    #     Emit the first element only and stop the Flowable sequence thereafter.
    #     """
    #
    #     ...

    @abstractmethod
    def flat_map(
            self,
            func: Callable[[Any], FlowableMixin],
            stack: List[FrameSummary],
    ) -> FlowableMixin:
        """
        Apply a function to each item emitted by the source and flattens the result.

        The specified function must return a Flowable. The resulting Flowable
        concatenates the elements of each inner Flowables.
        The resulting Flowable concatenates the items of each inner Flowable.
        """

        ...

    @abstractmethod
    def map(self, func: Callable[[Any], Any]) -> FlowableMixin:
        """ Map each element emitted by the source by applying the given function.

        :param func: function that defines the mapping applied to each element of the \
        Flowable sequence.
        """

        ...

    @abstractmethod
    def map_to_iterator(
            self,
            func: Callable[[ValueType], Iterator[ValueType]],
    ):
        """
        Create a Flowable that maps each element emitted by the source to an iterator
        and emits each element of these iterators.

        :param func: function that defines the mapping applied to each element to an iterator.
        """

        ...

    @abstractmethod
    def merge(self, *others: FlowableMixin) -> FlowableMixin:
        """
        Merge the elements of this and the other Flowable sequences into a single *Flowable*.

        :param sources: other Flowables that get merged to this Flowable.
        """

        ...

    @abstractmethod
    def observe_on(self, scheduler: Scheduler) -> FlowableMixin:
        """
        Schedule elements emitted by the source on a dedicated scheduler.

        :param scheduler: a rxbackpressure scheduler
        :return: an Flowable running on specified scheduler
        """

        ...

    @abstractmethod
    def pairwise(self) -> FlowableMixin:
        """
        Create a Flowable that emits a pair for each consecutive pairs of elements
        in the Flowable sequence.
        """

        ...

    @abstractmethod
    def reduce(
            self,
            func: Callable[[Any, Any], Any],
            initial: Any,
    ) -> FlowableMixin:
        """
        Apply an accumulator function over a Flowable sequence and emits a single element.

        :param func: An accumulator function to be invoked on each element
        :param initial: The initial accumulator value
        :return: a Flowable that emits the final accumulated value
        """

        ...

    @abstractmethod
    def repeat_first(self) -> FlowableMixin:
        """
        Return a Flowable that repeats the first element it receives from the source
        forever (until disposed).
        """

        ...

    @abstractmethod
    def scan(self, func: Callable[[Any, Any], Any], initial: Any) -> FlowableMixin:
        """
        Apply an accumulator function over a Flowable sequence and return each intermediate result.

        The initial value is used as the initial accumulator value.

        :param func: An accumulator function to be invoked on each element
        :param initial: The initial accumulator value
        :return: a Flowable that emits the accumulated values
        """

        ...

    def share(self) -> FlowableMixin:
        """
        Broadcast the elements of the Flowable to possibly multiple subscribers.

        This function is only valid when used inside a Multicast. Otherwise, it
        raise an exception.
        """

        raise Exception('this Flowable cannot be shared. Use multicasting to share Flowables.')

    @abstractmethod
    def to_list(self) -> FlowableMixin:
        """
        Create a new Flowable that collects the elements from the source sequence,
        and emits a single element of type List.
        """

        ...

    @abstractmethod
    def zip(self, *others: FlowableMixin) -> FlowableMixin:
        """
        Create a new Flowable from one or more Flowables by combining their item in pairs in a strict sequence.

        :param others: :param sources: other Flowables that get zipped to this Flowable.
        """

        ...

    @abstractmethod
    def zip_with_index(self) -> FlowableMixin:
        """
        Zip each item emitted by the source with the enumerated index.
        """

        ...
