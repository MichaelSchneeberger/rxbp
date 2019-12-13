from typing import Any, Callable

from rxbp.flowable import Flowable
from rxbp.flowablebase import FlowableBase
from rxbp.flowableoperator import FlowableOperator
from rxbp.flowableopmixin import FlowableOpMixin
from rxbp.scheduler import Scheduler


def buffer(buffer_size: int):
    def func(source: FlowableOpMixin):
        return source.buffer(buffer_size=buffer_size)

    return FlowableOperator(func)


def concat(*sources: FlowableBase):
    """ Consecutively subscribe each Flowable in a batch after all Flowables of the previous batch complete

    :param sources:
    :return:
    """

    def func(left: FlowableOpMixin):
        return left.concat(*sources)

    return FlowableOperator(func)


def controlled_zip(
        right: FlowableBase,
        request_left: Callable[[Any, Any], bool] = None,
        request_right: Callable[[Any, Any], bool] = None,
        match_func: Callable[[Any, Any], bool] = None,
):
    """ Creates a new observable from two observables by combining their item in pairs in a controlled manner

    :param right: other observable
    :param request_left: a function that returns True, if a new element from the left observable is requested \
    to build the the next pair
    :param request_right: a function that returns True, if a new element from the left observable is requested \
    to build the the next pair
    :param match_func: a filter function that returns True, if the current pair is sent downstream
    :return: zipped observable
    """

    def func(left: FlowableOpMixin):
        return left.controlled_zip(
            right=right,
            request_left=request_left,
            request_right=request_right,
            match_func=match_func,
        )

    return FlowableOperator(func)


def debug(name=None, on_next=None, on_subscribe=None, on_ack=None, on_raw_ack=None, on_ack_msg=None):
    def func(source: FlowableOpMixin):
        """ Prints debug messages to the console when providing the name argument

        :param source:
        :return:
        """

        return source.debug(
            name=name,
            on_next=on_next,
            on_subscribe=on_subscribe,
            on_ack=on_ack,
            on_raw_ack=on_raw_ack,
            on_ack_msg=on_ack_msg,
        )

    return FlowableOperator(func)


# def execute_on(scheduler: Scheduler):
#     def func(source: FlowableOpMixin):
#         return source.execute_on(scheduler=scheduler)
#
#     return FlowableOperator(func)


def fast_filter(predicate: Callable[[Any], bool]):
    """ Only emits those items for which the given predicate holds

    :param predicate: a function that returns True, if the current element passes the filter
    :return: filtered Flowable
    """

    def func(left: FlowableOpMixin):
        return left.fast_filter(predicate=predicate)

    return FlowableOperator(func)


def filter(predicate: Callable[[Any], bool]):
    """ Only emits those items for which the given predicate holds

    :param predicate: a function that returns True, if the current element passes the filter
    :return: filtered Flowable
    """

    def func(left: FlowableOpMixin):
        return left.filter(predicate=predicate)

    return FlowableOperator(func)


def filter_with_index(predicate: Callable[[Any, int], bool]):
    """ Only emits those items for which the given predicate holds

    :param predicate: a function that returns True, if the current element passes the filter
    :return: filtered Flowable
    """

    def func(left: FlowableOpMixin):
        return left.filter_with_index(predicate=predicate)

    return FlowableOperator(func)


def first(raise_exception: Callable[[Callable[[], None]], None] = None):
    """ Maps each item emitted by the source by applying the given function

    :param selector: function that defines the mapping applied to each element
    :return: mapped Flowable
    """

    def func(source: FlowableOpMixin):
        return source.first(raise_exception=raise_exception)

    return FlowableOperator(func)


def flat_map(selector: Callable[[Any], Flowable]):
    """ Applies a function to each item emitted by the source and flattens the result. The function takes any type
    as input and returns an inner observable. The resulting observable concatenates the items of each inner
    observable.

    :param selector: A function that takes any type as input and returns an Flowable.
    :return: a flattened Flowable
    """

    def func(left: FlowableOpMixin):
        return left.flat_map(selector=selector)

    return FlowableOperator(func)


def map(selector: Callable[[Any], Any]):
    """ Maps each item emitted by the source by applying the given function

    :param selector: function that defines the mapping applied to each element
    :return: mapped Flowable
    """

    def func(source: FlowableOpMixin):
        return source.map(selector=selector)

    return FlowableOperator(func)


def match(*others: Flowable):
    """ Creates a new flowable from two flowables by combining their item in pairs in a strict sequence.

    :param selector: a mapping function applied over the generated pairs
    :return: zipped Flowable
    """

    def func(left: FlowableOpMixin):
        return left.match(*others)

    return FlowableOperator(func)


def merge(*others: Flowable):
    """ Maps each item emitted by the source by applying the given function

    :param selector: function that defines the mapping applied to each element
    :return: mapped Flowable
    """

    def func(left: FlowableOpMixin):
        return left.merge(*others)

    return FlowableOperator(func)


def observe_on(scheduler: Scheduler):
    """ Operator that specifies a specific scheduler, on which observers will observe events

    :param scheduler: a rxbackpressure scheduler
    :return: an Flowable running on specified scheduler
    """

    def func(source: FlowableOpMixin):
        return source.observe_on(scheduler=scheduler)

    return FlowableOperator(func)


def pairwise():
    """ Creates a Flowable that pairs each neighbouring two items from the source

    :param selector: (optional) selector function
    :return: paired Flowable
    """

    def func(source: FlowableOpMixin):
        return source.pairwise()

    return FlowableOperator(func)


def repeat_first():
    """ Returns a flowable that repeats the first item it receives forever.

    :return: a flowable
    """

    def func(source: FlowableOpMixin):
        return source.repeat_first()

    return FlowableOperator(func)


def scan(func: Callable[[Any, Any], Any], initial: Any):
    """ Applies an accumulator function over a flowable sequence and
    returns each intermediate result. The initial value is used
    as the initial accumulator value.

    :param func: An accumulator function to be invoked on each element
    :param initial: The initial accumulator value
    :return: a flowable that emits the accumulated values
    """

    def inner_func(source: FlowableOpMixin):
        return source.scan(func=func, initial=initial)

    return FlowableOperator(inner_func)


def set_base(val: Any):
    def func(source: FlowableOpMixin):
        return source.set_base(val=val)

    return FlowableOperator(func)


def to_list():
    def func(source: FlowableOpMixin):
        return source.to_list()

    return FlowableOperator(func)


def zip(*others: Flowable):
    """ Creates a new flowable from two flowables by combining their item in pairs in a strict sequence.

    :param selector: a mapping function applied over the generated pairs
    :return: zipped Flowable
    """

    def func(left: FlowableOpMixin):
        return left.zip(*others)

    return FlowableOperator(func)


def zip_with_index():
    """ Zips each item emmited by the source with their indices

    :return: zipped with index Flowable
    """

    def func(left: FlowableOpMixin):
        return left.zip_with_index()

    return FlowableOperator(func)
