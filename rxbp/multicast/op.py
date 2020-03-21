from typing import Callable, Any

import rx

from rxbp.multicast.flowableop import FlowableOp
from rxbp.multicast.liftedmulticast import LiftedMultiCast
from rxbp.multicast.multicast import MultiCast
from rxbp.multicast.multicastoperator import MultiCastOperator
from rxbp.multicast.multicastopmixin import MultiCastOpMixin
from rxbp.multicast.typing import MultiCastValue
from rxbp.typing import ValueType


def join_flowables(
      *others: MultiCastOpMixin,
):
    """
    Zip one or more *Multicasts* (each emitting a single *Flowable*) to a *Multicast*
    emitting a single element (tuple of *Flowables*).

    The number of Flowables contained in the tuple is equal to the number of collected
    MultiCasts.

    ::

        # collect two Flowables emitted by two different MultiCast
        rxbp.multicast.return_flowable(rxbp.range(10)).pipe(
            rxbp.multicast.op.join_flowables(
                rxbp.multicast.return_flowable(rxbp.range(10)),
            ),
        )

    :param others: other MultiCasts that all emit a single Flowable
    """

    def op_func(source: MultiCastOpMixin):
        return source.join_flowables(*others)

    return MultiCastOperator(op_func)


def debug(name: str = None):
    """
    Print debug messages to the console when providing the `name` argument
    """

    def op_func(source: MultiCastOpMixin):
        return source.debug(name=name)

    return MultiCastOperator(op_func)


def default_if_empty(
        lazy_val: Callable[[], Any],
):
    """
    Either emits the elements of the source or a single element
    returned by `lazy_val` if the source doesn't emit any elements.

    :param lazy_val: a function that returns the single elements
    """

    def op_func(source: MultiCastOpMixin):
        return source.default_if_empty(lazy_val=lazy_val)

    return MultiCastOperator(op_func)


def loop_flowables(
        func: Callable[[MultiCastValue], MultiCastValue],
        initial: ValueType,
):
    """
    Create a MultiCast of multi-casted Flowables that are either emitted by the
    source or looped back inside the `loop_flowables`. The looped Flowable sequences
    are defined by an initial value and some recursive logic.

    :param func: a function that exposes (a shared MultiCast emitting)
    a dictionary of Flowables consisting of the incoming Flowables
    and the looped Flowables.
    :param initial: initial values of the looped Flowables
    """

    def op_func(source: MultiCastOpMixin):
        return source.loop_flowables(func=func, initial=initial)

    return MultiCastOperator(func=op_func)


def filter(
        predicate: Callable[[MultiCastValue], bool],
):
    """
    Emit only those MultiCast for which the given predicate hold.
    """

    def op_func(source: MultiCastOpMixin):
        return source.filter(predicate=predicate)

    return MultiCastOperator(op_func)


def first(
        raise_exception: Callable[[Callable[[], None]], None],
):
    """
    Either emits the elements of the source or a single element
    returned by `lazy_val` if the source doesn't emit any elements.

    :param lazy_val: a function that returns the single elements
    """

    def op_func(source: MultiCastOpMixin):
        return source.first(raise_exception=raise_exception)

    return MultiCastOperator(op_func)


def first_or_default(lazy_val: Callable[[], Any]):
    """
    Emit the first element only and stop the Flowable sequence thereafter.
    """

    def op_func(source: MultiCastOpMixin):
        return source.first_or_default(lazy_val=lazy_val)

    return MultiCastOperator(op_func)


def flat_map(func: Callable[[MultiCastValue], MultiCastOpMixin]):
    """
    Apply a function to each item emitted by the source and flattens the result.
    """

    def op_func(source: MultiCastOpMixin):
        return source.flat_map(func=func)

    return MultiCastOperator(op_func)


def lift(
    func: Callable[[MultiCast, MultiCastValue], MultiCastValue],
):
    """
    Lift the current `MultiCast[T]` to a `MultiCast[MultiCast[T]]`.
    """

    def op_func(source: MultiCastOpMixin):
        return source.lift(func=func).map(lambda m: LiftedMultiCast(m))

    return MultiCastOperator(op_func)


def map(func: Callable[[MultiCastValue], MultiCastValue]):
    """
    Map each element emitted by the source by applying the given function.
    """

    def op_func(source: MultiCastOpMixin):
        return source.map(func=func)

    return MultiCastOperator(op_func)


def map_with_op(func: Callable[[MultiCastValue, FlowableOp], MultiCastValue]):
    """
    Maps each `MultiCast` value by applying the given function `func`
    """

    def op_func(source: MultiCastOpMixin):
        return source.map_with_op(func=func)

    return MultiCastOperator(op_func)


def merge(*others: MultiCastOpMixin):
    """
    Merge the elements of the *Flowable* sequences into a single *Flowable*.
    """

    def op_func(source: MultiCastOpMixin):
        return source.merge(*others)

    return MultiCastOperator(op_func)


def observe_on(scheduler: rx.typing.Scheduler):
    """
    Schedule elements emitted by the source on a dedicated scheduler
    """

    def op_func(source: MultiCastOpMixin):
        return source.observe_on(scheduler=scheduler)

    return MultiCastOperator(op_func)


def collect_flowables(
    maintain_order: bool = None,
):
    """
    Create a MultiCast that emits a single element containing the reduced Flowables
    of the first element sent by the source. It is expected that the consequent
    elements emitted by the source have the same structure as the first element.

    A reduced Flowable sequences is composed by one or more (Flowable) sources.

    :param maintain_order: if True, then the reduced Flowable sequences maintain
    the order of the Flowable sources. Otherwise, the reduced Flowable
    sequence flattens the elements emitted by the sources.
    """

    def op_func(source: MultiCastOpMixin):
        return source.collect_flowables(maintain_order=maintain_order)

    return MultiCastOperator(op_func)


def share(
        func: Callable[[MultiCast], MultiCast],
):
    """
    Multi-cast the elements of the source to possibly multiple subscribers.

    This function is only valid when used inside a Multicast. Otherwise, it
    raise an exception.
    """

    def op_func(source: MultiCastOpMixin):
        return func(source._share())

    return MultiCastOperator(op_func)
