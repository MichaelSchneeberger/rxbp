from typing import Any, Callable, Iterator

from rxbp.flowable import Flowable
from rxbp.flowablebase import FlowableBase
from rxbp.flowableoperator import FlowableOperator
from rxbp.flowableopmixin import FlowableOpMixin
from rxbp.scheduler import Scheduler
from rxbp.typing import ValueType


def buffer(buffer_size: int = None):
    """
    Buffer the element emitted by the source without back-pressure until the buffer is full.
    """

    def op_func(source: FlowableOpMixin):
        return source.buffer(buffer_size=buffer_size)

    return FlowableOperator(op_func)


def concat(*sources: FlowableBase):
    """
    Concatentates Flowables sequences together by back-pressuring the tail Flowables until
    the current Flowable has completed.

    :param sources: other Flowables that get concatenate to this Flowable.
    """

    def op_func(left: FlowableOpMixin):
        return left.concat(*sources)

    return FlowableOperator(op_func)


def controlled_zip(
        right: FlowableBase,
        request_left: Callable[[Any, Any], bool] = None,
        request_right: Callable[[Any, Any], bool] = None,
        match_func: Callable[[Any, Any], bool] = None,
):
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

    def op_func(left: FlowableOpMixin):
        return left.controlled_zip(
            right=right,
            request_left=request_left,
            request_right=request_right,
            match_func=match_func,
        )

    return FlowableOperator(op_func)


def debug(name=None, on_next=None, on_subscribe=None, on_ack=None, on_raw_ack=None, on_ack_msg=None):
    """
    Print debug messages to the console when providing the `name` argument

    :on_next: customize the on next debug console print
    """

    def op_func(source: FlowableOpMixin):
        return source.debug(
            name=name,
            on_next=on_next,
            on_subscribe=on_subscribe,
            on_ack=on_ack,
            on_raw_ack=on_raw_ack,
            on_ack_msg=on_ack_msg,
        )

    return FlowableOperator(op_func)


def default_if_empty(lazy_val: Callable[[], Any]):
    """
    Only the elements of the source or a default value if the source is an empty sequence

    :param lazy_val: a function that returns the default value
    """

    def op_func(left: FlowableOpMixin):
        return left.default_if_empty(lazy_val=lazy_val)

    return FlowableOperator(op_func)


def do_action(
        on_next: Callable[[Any], None] = None,
        on_completed: Callable[[], None] = None,
        on_error: Callable[[Exception], None] = None,
        on_disposed: Callable[[], None] = None,
):
    def op_func(left: FlowableOpMixin):
        return left.do_action(
            on_next=on_next,
            on_completed=on_completed,
            on_error=on_error,
            on_disposed=on_disposed,
        )

    return FlowableOperator(op_func)


def execute_on(scheduler: Scheduler):
    """
    Inject new scheduler that is used to subscribe the Flowable.
    """

    def op_func(left: FlowableOpMixin):
        return left.execute_on(scheduler=scheduler)

    return FlowableOperator(op_func)


def fast_filter(predicate: Callable[[Any], bool]):
    """
    Only emit those elements for which the given predicate holds.

    :param predicate: a function that returns True, if the current element passes the filter
    :return: filtered Flowable
    """

    def op_func(left: FlowableOpMixin):
        return left.fast_filter(predicate=predicate)

    return FlowableOperator(op_func)


def filter(predicate: Callable[[Any], bool]):
    """
    Only emit those elements for which the given predicate holds.

    :param predicate: a function that returns True, if the current element passes the filter
    :return: filtered Flowable
    """

    def op_func(left: FlowableOpMixin):
        return left.filter(predicate=predicate)

    return FlowableOperator(op_func)


def first(raise_exception: Callable[[Callable[[], None]], None]):
    """
    Emit the first element only and stop the Flowable sequence thereafter.

    The `raise_exception` argument needs to be given as follows:

    ::

        rxbp.op.first(raise_exception=lambda f: f())

    The `raise_exception` argument is called in case there is no first element.
    Like RxPY, a SequenceContainsNoElementsError is risen. But on top of that
    the place where the `first` argument is used is added to the traceback by
    providing the `raise_exception` argument.
    """

    def op_func(source: FlowableOpMixin):
        return source.first(raise_exception=raise_exception)

    return FlowableOperator(op_func)


def first_or_default(lazy_val: Callable[[], Any]):
    """
    Emit the first element only and stop the Flowable sequence thereafter.
    """

    def op_func(source: FlowableOpMixin):
        return source.first_or_default(lazy_val=lazy_val)

    return FlowableOperator(op_func)


def flat_map(func: Callable[[Any], Flowable]):
    """
    Apply a function to each item emitted by the source and flattens the result.

    The specified function must return a Flowable. The resulting Flowable
    concatenates the elements of each inner Flowables.
    The resulting Flowable concatenates the items of each inner Flowable.
    """

    def op_func(source: FlowableOpMixin):
        return source.flat_map(func=func)

    return FlowableOperator(op_func)


def map(func: Callable[[Any], Any]):
    """ Map each element emitted by the source by applying the given function.

    :param func: function that defines the mapping applied to each element of the \
    Flowable sequence.
    """

    def op_func(source: FlowableOpMixin):
        return source.map(func=func)

    return FlowableOperator(op_func)


def map_to_iterator(
        func: Callable[[ValueType], Iterator[ValueType]],
):
    """
    Create a Flowable that maps each element emitted by the source to an iterator
    and emits each element of these iterators.

    :param func: function that defines the mapping applied to each element to an iterator.
    """

    def op_func(source: FlowableOpMixin):
        return source.map_to_iterator(func=func)

    return FlowableOperator(op_func)


def match(*others: Flowable):
    """
    Create a new Flowable from this and other Flowables by first filtering and duplicating (if necessary)
    the elements of each Flowable and zip the resulting Flowable sequences together.

    :param sources: other Flowables that get matched with this Flowable.
    """

    def op_func(left: FlowableOpMixin):
        return left.match(*others)

    return FlowableOperator(op_func)


def merge(*others: Flowable):
    """
    Merge the elements of this and the other Flowable sequences into a single *Flowable*.

    :param sources: other Flowables that get merged to this Flowable.
    """

    def op_func(left: FlowableOpMixin):
        return left.merge(*others)

    return FlowableOperator(op_func)


def observe_on(scheduler: Scheduler):
    """
    Schedule elements emitted by the source on a dedicated scheduler.

    :param scheduler: a rxbackpressure scheduler
    :return: an Flowable running on specified scheduler
    """

    def op_func(source: FlowableOpMixin):
        return source.observe_on(scheduler=scheduler)

    return FlowableOperator(op_func)


def pairwise():
    """
    Create a Flowable that emits a pair for each consecutive pairs of elements
    in the Flowable sequence.
    """

    def op_func(source: FlowableOpMixin):
        return source.pairwise()

    return FlowableOperator(op_func)


def reduce(
        func: Callable[[Any, Any], Any],
        initial: Any,
):
    """
    Apply an accumulator function over a Flowable sequence and emits a single element.

    :param func: An accumulator function to be invoked on each element
    :param initial: The initial accumulator value
    :return: a Flowable that emits the final accumulated value
    """

    def op_func(source: FlowableOpMixin):
        return source.reduce(func=func, initial=initial)

    return FlowableOperator(op_func)


def repeat_first():
    """
    Return a Flowable that repeats the first element it receives from the source
    forever (until disposed).
    """

    def op_func(source: FlowableOpMixin):
        return source.repeat_first()

    return FlowableOperator(op_func)


def scan(func: Callable[[Any, Any], Any], initial: Any):
    """
    Apply an accumulator function over a Flowable sequence and return each intermediate result.

    The initial value is used as the initial accumulator value.

    :param func: An accumulator function to be invoked on each element
    :param initial: The initial accumulator value
    :return: a Flowable that emits the accumulated values
    """

    def op_func(source: FlowableOpMixin):
        return source.scan(func=func, initial=initial)

    return FlowableOperator(op_func)


def share():
    """
    Broadcast the elements of the Flowable to possibly multiple subscribers.

    This function is only valid when used inside a Multicast. Otherwise, it
    raise an exception.
    """

    def inner_func(source: FlowableOpMixin):
        return source.share()#bind_to=ability)

    return FlowableOperator(inner_func)


def set_base(val: Any):
    """
    Overwrite the base of the current Flowable sequence.
    """

    def op_func(source: FlowableOpMixin):
        return source.set_base(val=val)

    return FlowableOperator(op_func)


def to_list():
    """
    Create a new Flowable that collects the elements from the source sequence,
    and emits a single element of type List.
    """

    def op_func(source: FlowableOpMixin):
        return source.to_list()

    return FlowableOperator(op_func)


def zip(*others: Flowable):
    """
    Create a new Flowable from one or more Flowables by combining their item in pairs in a strict sequence.

    :param others: :param sources: other Flowables that get zipped to this Flowable.
    """

    def op_func(left: FlowableOpMixin):
        return left.zip(*others)

    return FlowableOperator(op_func)


def zip_with_index():
    """
    Zip each item emitted by the source with the enumerated index.
    """

    def op_func(left: FlowableOpMixin):
        return left.zip_with_index()

    return FlowableOperator(op_func)
