from typing import Any, Callable, Iterator

from rxbp.flowable import Flowable
from rxbp.flowablebase import FlowableBase
from rxbp.flowableoperator import FlowableOperator
from rxbp.flowableopmixin import FlowableOpMixin
from rxbp.scheduler import Scheduler
from rxbp.typing import ValueType


def buffer(buffer_size: int):
    def op_func(source: FlowableOpMixin):
        return source.buffer(buffer_size=buffer_size)

    return FlowableOperator(op_func)


def concat(*sources: FlowableBase):
    """ Concatentates Flowables sequences together by back-pressuring the tail Flowables until
    the current Flowable completed

    :param sources: other Flowables that get concatenate to the selected Flowable
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
    Creates a new Flowable from two Flowables by combining their items in pairs. Which
    item gets paired with an item from the other Flowable is determined by two functions
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
    def op_func(source: FlowableOpMixin):
        """ Prints debug messages to the console when providing the `name` argument

        :on_next: customize the on next depug print
        """

        return source.debug(
            name=name,
            on_next=on_next,
            on_subscribe=on_subscribe,
            on_ack=on_ack,
            on_raw_ack=on_raw_ack,
            on_ack_msg=on_ack_msg,
        )

    return FlowableOperator(op_func)


def fast_filter(predicate: Callable[[Any], bool]):
    """ Only emits those items for which the given predicate holds

    :param predicate: a function that returns True, if the current element passes the filter
    :return: filtered Flowable
    """

    def op_func(left: FlowableOpMixin):
        return left.fast_filter(predicate=predicate)

    return FlowableOperator(op_func)


def filter(predicate: Callable[[Any], bool]):
    """ Only emits those items for which the given predicate holds.

    :param predicate: a function that returns True, if the current element passes the filter
    :return: filtered Flowable
    """

    def op_func(left: FlowableOpMixin):
        return left.filter(predicate=predicate)

    return FlowableOperator(op_func)


def filter_with_index(predicate: Callable[[Any, int], bool]):
    """ Only emits those items for which the given predicate holds

    :param predicate: a function that returns True, if the current element passes the filter
    :return: filtered Flowable
    """

    def op_func(left: FlowableOpMixin):
        return left.filter_with_index(predicate=predicate)

    return FlowableOperator(op_func)


def first(raise_exception: Callable[[Callable[[], None]], None] = None):
    """ Maps each item emitted by the source by applying the given function

    :param selector: function that defines the mapping applied to each element
    :return: mapped Flowable
    """

    def op_func(source: FlowableOpMixin):
        return source.first(raise_exception=raise_exception)

    return FlowableOperator(op_func)


def flat_map(func: Callable[[Any], Flowable]):
    """
    Applies a function to each item emitted by the source and flattens the result.
    The function takes the Flowable's value type as input and returns an inner Flowable.
    The resulting Flowable concatenates the items of each inner Flowable.

    :param selector: A function that takes any type as input and returns an Flowable.
    :return: a flattened Flowable
    """

    def op_func(source: FlowableOpMixin):
        return source.flat_map(func=func)

    return FlowableOperator(op_func)


def map(func: Callable[[Any], Any]):
    """ Maps each item emitted by the source by applying the given function.

    :param func: function that defines the mapping applied to each element of the \
    Flowable sequence.
    """

    def op_func(source: FlowableOpMixin):
        return source.map(func=func)

    return FlowableOperator(op_func)


def map_to_iterator(
        func: Callable[[ValueType], Iterator[ValueType]],
):
    """ Creates a Flowable that maps each item emitted by the source to an iterator
    and emits each element of the iterator.

    :param func: function that defines the mapping applied to each element to an iterator.
    """

    def op_func(source: FlowableOpMixin):
        return source.map_to_iterator(func=func)

    return FlowableOperator(op_func)


def match(*others: Flowable):
    """ Creates a new Flowable from two or more Flowables by zipping their elements in a
    matching manner.

    :param others:
    """

    def op_func(left: FlowableOpMixin):
        return left.match(*others)

    return FlowableOperator(op_func)


def merge(*others: Flowable):
    """ Maps each item emitted by the source by applying the given function

    :param selector: function that defines the mapping applied to each element
    :return: mapped Flowable
    """

    def op_func(left: FlowableOpMixin):
        return left.merge(*others)

    return FlowableOperator(op_func)


def observe_on(scheduler: Scheduler):
    """ Operator that specifies a specific scheduler, on which observers will observe events

    :param scheduler: a rxbackpressure scheduler
    :return: an Flowable running on specified scheduler
    """

    def op_func(source: FlowableOpMixin):
        return source.observe_on(scheduler=scheduler)

    return FlowableOperator(op_func)


def pairwise():
    """ Creates a Flowable that pairs each neighbouring two items from the source

    :param selector: (optional) selector function
    :return: paired Flowable
    """

    def op_func(source: FlowableOpMixin):
        return source.pairwise()

    return FlowableOperator(op_func)


def reduce(
        func: Callable[[Any, Any], Any],
        initial: Any,
):
    def op_func(source: FlowableOpMixin):
        return source.reduce(func=func, initial=initial)

    return FlowableOperator(op_func)


def repeat_first():
    """ Returns a flowable that repeats the first item it receives forever.

    :return: a flowable
    """

    def op_func(source: FlowableOpMixin):
        return source.repeat_first()

    return FlowableOperator(op_func)


def scan(func: Callable[[Any, Any], Any], initial: Any):
    """ Applies an accumulator function over a flowable sequence and
    returns each intermediate result. The initial value is used
    as the initial accumulator value.

    :param func: An accumulator function to be invoked on each element
    :param initial: The initial accumulator value
    :return: a flowable that emits the accumulated values
    """

    def op_func(source: FlowableOpMixin):
        return source.scan(func=func, initial=initial)

    return FlowableOperator(op_func)


def share(): #ability: MultiCastContext):
    """
    """

    def inner_func(source: FlowableOpMixin):
        return source.share()#bind_to=ability)

    return FlowableOperator(inner_func)


def set_base(val: Any):
    def op_func(source: FlowableOpMixin):
        return source.set_base(val=val)

    return FlowableOperator(op_func)


def to_list():
    def op_func(source: FlowableOpMixin):
        return source.to_list()

    return FlowableOperator(op_func)


def zip(*others: Flowable):
    """ Creates a new flowable from two flowables by combining their item in pairs in a strict sequence.

    :param selector: a mapping function applied over the generated pairs
    :return: zipped Flowable
    """

    def op_func(left: FlowableOpMixin):
        return left.zip(*others)

    return FlowableOperator(op_func)


def zip_with_index():
    """ Zips each item emmited by the source with their indices

    :return: zipped with index Flowable
    """

    def op_func(left: FlowableOpMixin):
        return left.zip_with_index()

    return FlowableOperator(op_func)
