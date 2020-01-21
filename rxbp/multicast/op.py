from typing import Callable, Any

import rx

from rxbp.multicast.flowableop import FlowableOp
from rxbp.multicast.liftedmulticast import LiftedMultiCast
from rxbp.multicast.multicast import MultiCast
from rxbp.multicast.multicastoperator import MultiCastOperator
from rxbp.multicast.multicastopmixin import MultiCastOpMixin
from rxbp.multicast.typing import MultiCastValue
from rxbp.typing import ValueType


def debug(name: str):
    def op_func(source: MultiCastOpMixin):
        return source.debug(name=name)

    return MultiCastOperator(op_func)


def default_if_empty(
        val: Any,
):
    def op_func(source: MultiCastOpMixin):
        return source.default_if_empty(val=val)

    return MultiCastOperator(op_func)


def collect_flowables(
      *others: MultiCastOpMixin,
):
    def op_func(source: MultiCastOpMixin):
        return source.collect_flowables(*others)

    return MultiCastOperator(op_func)


# todo: add loop
# def loop(
#         func: Callable[[MultiCastValue], MultiCastValue],
# ):
#     pass

def loop_flowable(
        func: Callable[[MultiCastValue], MultiCastValue],
        initial: ValueType,
):
    def op_func(source: MultiCastOpMixin):
        return source.loop_flowable(func=func, initial=initial)

    return MultiCastOperator(func=op_func)


def filter(
        predicate: Callable[[MultiCastValue], bool],
):
    """ Only emits those `MultiCast` values for which the given predicate hold.
    """

    def op_func(source: MultiCastOpMixin):
        return source.filter(predicate=predicate)

    return MultiCastOperator(op_func)


def flat_map(func: Callable[[MultiCastValue], MultiCastOpMixin]):
    """ Maps each `MultiCast` value by applying the given function `func` and flattens the result.
    """

    def op_func(source: MultiCastOpMixin):
        return source.flat_map(func=func)

    return MultiCastOperator(op_func)


def lift(
    func: Callable[[MultiCast, MultiCastValue], MultiCastValue],
):
    """ Lift the current `MultiCast[T]` to a `MultiCast[MultiCast[T]]`.
    """

    def op_func(source: MultiCastOpMixin):
        return source.lift(func=func).map(lambda m: LiftedMultiCast(m))

    return MultiCastOperator(op_func)


def merge(*others: MultiCastOpMixin):
    """ Merges two or more `MultiCast` streams together
    """

    def op_func(source: MultiCastOpMixin):
        return source.merge(*others)

    return MultiCastOperator(op_func)


def map(func: Callable[[MultiCastValue], MultiCastValue]):
    """ Maps each `MultiCast` value by applying the given function `func`
    """

    def op_func(source: MultiCastOpMixin):
        return source.map(func=func)

    return MultiCastOperator(op_func)


def map_with_op(func: Callable[[MultiCastValue, FlowableOp], MultiCastValue]):
    """ Maps each `MultiCast` value by applying the given function `func`
    """

    def op_func(source: MultiCastOpMixin):
        return source.map_with_op(func=func)

    return MultiCastOperator(op_func)


# def map_to_iterator(func: Callable[[MultiCastValue], Iterator[MultiCastValue]]):
#     """ Maps each `MultiCast` value by applying the given function `func`
#     """
#
#     def op_func(source: MultiCastOpMixin):
#         return source.map_to_iterator(func=func)
#
#     return MultiCastOperator(op_func)


def observe_on(scheduler: rx.typing.Scheduler):
    def op_func(source: MultiCastOpMixin):
        return source.observe_on(scheduler=scheduler)

    return MultiCastOperator(op_func)


def reduce_flowable(
    maintain_order: bool = None,
):
    """ Lift the current `MultiCast[ReducableMixin[T]]` to a `MultiCast[ReducableMixin[T]]`.
    """

    def op_func(source: MultiCastOpMixin):
        return source.reduce_flowable(maintain_order=maintain_order)

    return MultiCastOperator(op_func)


# def share():
#     """ Splits the `MultiCast` stream in two, applies the given `MultiCast` operators on each of them, and merges the
#     two streams together again.
#     """
#
#     def op_func(source: MultiCastOpMixin):
#         return source.share()
#
#     return MultiCastOperator(op_func)


def share(
        func: Callable[[MultiCast], MultiCast],
):
    """ Splits the `MultiCast` stream in two, applies the given `MultiCast` operators on each of them, and merges the
    two streams together again.
    """

    def op_func(source: MultiCastOpMixin):
        return func(source._share())

    return MultiCastOperator(op_func)
