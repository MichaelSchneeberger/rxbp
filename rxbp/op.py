from typing import Any, Callable, Iterable, Iterator

from rxbp.flowable import Flowable
from rxbp.scheduler import Scheduler
from rxbp.flowablebase import FlowableBase
from rxbp.flowableoperator import FlowableOperator


# def buffer(size: int):
#     def func(obs: ObservableBase):
#         class BufferObservable(ObservableBase):
#             def unsafe_subscribe(self, observer, scheduler, subscribe_scheduler):
#                 buffered_subscriber = BufferedSubscriber(
#                     observer=observer, scheduler=scheduler, buffer_size=size)
#                 disposable = obs.unsafe_subscribe(buffered_subscriber, scheduler, subscribe_scheduler)
#                 return disposable
#         return BufferObservable()
#     return ObservableOperator(func)


# def cache():
#     """ Converts this observable into a multicast observable that caches the items that the fastest observer has
#     already received and the slowest observer has not yet requested. Note that this observable is subscribed when
#     the multicast observable is subscribed for the first time. Therefore, this observable is never subscribed more
#     than once.
#
#     :return: multicast observable
#     """
#
#     def func(obs: ObservableBase):
#         return ConnectableObservable(source=obs, subject=CachedServeFirstSubject()).ref_count()
#     return ObservableOperator(func)


def concat(sources: Iterable[FlowableBase]):
    def func(left: Flowable) -> FlowableBase:
        return left.concat(sources=sources)
    return FlowableOperator(func)

def debug(name=None, on_next=None, on_subscribe=None, on_ack=None, on_raw_ack=None, on_ack_msg=None):
    def func(source: Flowable) -> FlowableBase:
        return source.debug(name=name, on_next=on_next, on_subscribe=on_subscribe, on_ack=on_ack, on_raw_ack=on_raw_ack,
                            on_ack_msg=on_ack_msg)
    return FlowableOperator(func)

def execute_on(scheduler: Scheduler):
    def func(source: Flowable) -> FlowableBase:
        return source.execute_on(scheduler=scheduler)
    return FlowableOperator(func)

def controlled_zip(right: FlowableBase,
                   request_left: Callable[[Any, Any], bool],
                   request_right: Callable[[Any, Any], bool],
                   match_func: Callable[[Any, Any], bool], ):
    """ Creates a new observable from two observables by combining their item in pairs in a controlled manner

    :param right: other observable
    :param request_left: a function that returns True, if a new element from the left observable is requested \
    to build the the next pair
    :param request_right: a function that returns True, if a new element from the left observable is requested \
    to build the the next pair
    :param match_func: a filter function that returns True, if the current pair is sent downstream
    :return: zipped observable
    """

    def func(left: Flowable) -> FlowableBase:
        return left.controlled_zip(right=right, request_left=request_left, request_right=request_right, match_func=match_func)
    return FlowableOperator(func)


def filter(predicate: Callable[[Any], bool]):
    """ Only emits those items for which the given predicate holds

    :param predicate: a function that returns True, if the current element passes the filter
    :return: filtered observable
    """

    def func(left: Flowable) -> FlowableBase:
        return left.filter(predicate=predicate)
    return FlowableOperator(func)


def flat_map(selector: Callable[[Any], FlowableBase]):
    """ Applies a function to each item emitted by the source and flattens the result. The function takes any type
    as input and returns an inner observable. The resulting observable concatenates the items of each inner
    observable.

    :param selector: A function that takes any type as input and returns an observable.
    :return: a flattened observable
    """

    def func(left: Flowable) -> FlowableBase:
        return left.flat_map(selector=selector)
    return FlowableOperator(func)


# def flat_zip(right: ObservableBase, inner_selector: Callable[[Any], ObservableBase], left_selector: Callable[[Any], Any]=None,
#              result_selector: Callable[[Any, Any, Any], Any] = None):
#     def func(obs: ObservableBase):
#         return FlatZipObservable(left=obs, right=right,
#                                  inner_selector=inner_selector, left_selector=left_selector,
#                                  result_selector=result_selector)
#     return ObservableOperator(func)


def map(selector: Callable[[Any], Any]):
    """ Maps each item emitted by the source by applying the given function

    :param selector: function that defines the mapping applied to each element
    :return: mapped observable
    """

    def func(source: Flowable) -> FlowableBase:
        return source.map(selector=selector)
    return FlowableOperator(func)


def merge(other: FlowableBase):
    """ Maps each item emitted by the source by applying the given function

    :param selector: function that defines the mapping applied to each element
    :return: mapped observable
    """

    def func(source: Flowable) -> FlowableBase:
        return source.merge(other=other)
    return FlowableOperator(func)


def observe_on(scheduler: Scheduler):
    """ Operator that specifies a specific scheduler, on which observers will observe events

    :param scheduler: a rxbackpressure scheduler
    :return: an observable running on specified scheduler
    """

    def func(source: Flowable) -> FlowableBase:
        return source.observe_on(scheduler=scheduler)
    return FlowableOperator(func)


# def pairwise():
#     """ Creates an observable that pairs each neighbouring two items from the source
#
#     :param selector: (optional) selector function
#     :return: paired observable
#     """
#
#     def func(obs: ObservableBase):
#         return PairwiseObservable(source=obs)
#     return ObservableOperator(func)


def repeat_first():
    """ Returns a flowable that repeats the first item it receives forever.

    :return: a flowable
    """

    def func(source: Flowable) -> FlowableBase:
        return source.repeat_first()
    return FlowableOperator(func)


# def replay():
#     """ Converts this observable into a multicast observable that replays the item received by the source. Note
#     that this observable is subscribed when the multicast observable is subscribed for the first time. Therefore,
#     this observable is never subscribed more than once.
#
#     :return: multicast observable
#     """
#
#     def func(obs: ObservableBase):
#         observable = ConnectableObservable(
#             source=obs,
#             subject=ReplaySubject()
#         ).ref_count()
#         return observable
#     return ObservableOperator(func)


def scan(func: Callable[[Any, Any], Any], initial: Any):
    """ Applies an accumulator function over a flowable sequence and
    returns each intermediate result. The initial value is used
    as the initial accumulator value.

    :param func: An accumulator function to be invoked on each element
    :param initial: The initial accumulator value
    :return: a flowable that emits the accumulated values
    """

    def inner_func(source: Flowable) -> FlowableBase:
        return source.scan(func=func, initial=initial)
    return FlowableOperator(inner_func)


def share(func: Callable[[FlowableBase], FlowableBase]):
    """ Share takes a function and exposes a multi-cast flowable via the function's arguments. The multi-cast
    flowable back-pressures, when the first subscriber back-pressures. In case of more than one subscribers,
    the multi-cast flowable buffers the elements and releases an element when the slowest subscriber back-pressures
    the element.

    :return: flowable returned by the share function
    """

    def inner_func(source: Flowable) -> FlowableBase:
        return source.share(func=func)
    return FlowableOperator(inner_func)


def zip(right: FlowableBase, selector: Callable[[Any, Any], Any] = None):
    """ Creates a new flowable from two flowables by combining their item in pairs in a strict sequence.

    :param selector: a mapping function applied over the generated pairs
    :return: zipped observable
    """

    def func(left: Flowable) -> FlowableBase:
        return left.zip(right=right, selector=selector)
    return FlowableOperator(func)


def match(right: FlowableBase, selector: Callable[[Any, Any], Any] = None):
    """ Creates a new flowable from two flowables by combining their item in pairs in a strict sequence.

    :param selector: a mapping function applied over the generated pairs
    :return: zipped observable
    """

    def func(left: Flowable) -> FlowableBase:
        return left.match(right=right, selector=selector)
    return FlowableOperator(func)


def zip_with_index(selector: Callable[[Any, int], Any] = None):
    """ Zips each item emmited by the source with their indices

    :param selector: a mapping function applied over the generated pairs
    :return: zipped with index observable
    """

    def func(left: Flowable) -> FlowableBase:
        return left.zip_with_index(selector=selector)
    return FlowableOperator(func)

