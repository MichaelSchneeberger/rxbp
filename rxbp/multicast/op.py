from typing import Callable, Any

import rx

from rxbp.multicast.init.initmulticast import init_multicast
from rxbp.multicast.mixins.liftindexmulticastmixin import LiftIndexMultiCastMixin
from rxbp.multicast.mixins.multicastmixin import MultiCastMixin
from rxbp.multicast.mixins.multicastopmixin import MultiCastOpMixin
from rxbp.multicast.multicast import MultiCast
from rxbp.multicast.multicastobserverinfo import MultiCastObserverInfo
from rxbp.multicast.multicastoperator import MultiCastOperator
from rxbp.multicast.multicastsubscriber import MultiCastSubscriber
from rxbp.multicast.typing import MultiCastItem
from rxbp.scheduler import Scheduler
from rxbp.typing import ValueType
from rxbp.utils.getstacklines import get_stack_lines


def assert_single_subscription():
    stack = get_stack_lines()

    def op_func(source: MultiCast):
        return source.assert_single_subscription(stack=stack)

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

    stack = get_stack_lines()

    def op_func(source: MultiCast):
        return source.collect_flowables(
            stack=stack,
            maintain_order=maintain_order,
        )

    return MultiCastOperator(op_func)


def merge_flowables(
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

    stack = get_stack_lines()

    def op_func(source: MultiCast):
        return source.collect_flowables(
            stack=stack,
            maintain_order=maintain_order,
        )

    return MultiCastOperator(op_func)


def debug(
        name: str,
        on_next: Callable[[Any], None] = None,
        on_completed: Callable[[], None] = None,
        on_error: Callable[[Exception], None] = None,
        on_subscribe: Callable[[MultiCastObserverInfo, MultiCastSubscriber], None] = None,
        on_observe: Callable[[MultiCastObserverInfo], None] = None,
        on_dispose: Callable[[], None] = None,
        verbose: bool = None,
):
    """
    Print debug messages to the console when providing the `name` argument
    """

    stack = get_stack_lines()

    def op_func(source: MultiCast):
        return source.debug(
            name=name,
            on_next=on_next,
            on_completed=on_completed,
            on_error=on_error,
            on_subscribe=on_subscribe,
            on_observe=on_observe,
            on_dispose=on_dispose,
            verbose=verbose,
            stack=stack,
        )

    return MultiCastOperator(op_func)


def default_if_empty(
        lazy_val: Callable[[], Any],
):
    """
    Either emits the elements of the source or a single element
    returned by `lazy_val` if the source doesn't emit any elements.

    :param lazy_val: a function that returns the single elements
    """

    def op_func(source: MultiCast):
        return source.default_if_empty(lazy_val=lazy_val)

    return MultiCastOperator(op_func)


def loop_flowables(
        func: Callable[[MultiCastItem], MultiCastItem],
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

    stack = get_stack_lines()

    def op_func(source: MultiCast):
        def lifted_func(multicast: MultiCastMixin):
            return func(init_multicast(multicast))

        return source.loop_flowables(
            func=lifted_func,
            initial=initial,
            stack=stack,
        )

    return MultiCastOperator(func=op_func)


def filter(
        predicate: Callable[[MultiCastItem], bool],
):
    """
    Emit only those MultiCast for which the given predicate hold.
    """

    def op_func(source: MultiCast):
        return source.filter(predicate=predicate)

    return MultiCastOperator(op_func)


def first(
        # raise_exception: Callable[[Callable[[], None]], None],
):
    """
    Either emits the elements of the source or a single element
    returned by `lazy_val` if the source doesn't emit any elements.

    :param lazy_val: a function that returns the single elements
    """

    stack = get_stack_lines()

    def op_func(source: MultiCast):
        return source.first(stack=stack)

    return MultiCastOperator(op_func)


def first_or_default(lazy_val: Callable[[], Any]):
    """
    Emit the first element only and stop the Flowable sequence thereafter.
    """

    def op_func(source: MultiCast):
        return source.first_or_default(lazy_val=lazy_val)

    return MultiCastOperator(op_func)


def flat_map(func: Callable[[MultiCastItem], MultiCastOpMixin]):
    """
    Apply a function to each item emitted by the source and flattens the result.
    """

    stack = get_stack_lines()

    def op_func(source: MultiCast):
        return source.flat_map(func=func, stack=stack)

    return MultiCastOperator(op_func)


def flatten():
    """
    Apply a function to each item emitted by the source and flattens the result.
    """

    stack = get_stack_lines()

    def op_func(source: MultiCast):
        return source.flat_map(func=lambda v: v, stack=stack)

    return MultiCastOperator(op_func)


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

    stack = get_stack_lines()

    def op_func(source: MultiCast):
        return source.join_flowables(list(others), stack=stack)

    return MultiCastOperator(op_func)


def lift():
    """
    Lift the current `MultiCast[T]` to a `MultiCast[MultiCast[T]]`.
    """

    def op_func(source: MultiCast):
        return source.lift()

    return MultiCastOperator(op_func)


def map(func: Callable[[MultiCastItem], MultiCastItem]):
    """
    Map each element emitted by the source by applying the given function.
    """

    def op_func(source: MultiCast):
        return source.map(func=func)

    return MultiCastOperator(op_func)


# def map_with_op(func: Callable[[MultiCastValue, FlowableOp], MultiCastValue]):
#     """
#     Maps each `MultiCast` value by applying the given function `func`
#     """
#
#     def op_func(source: MultiCast):
#         return source.map_with_op(func=func)
#
#     return MultiCastOperator(op_func)


def merge(*others: MultiCast):
    """
    Merge the elements of the *Flowable* sequences into a single *Flowable*.
    """

    def op_func(source: MultiCast):
        # lifted_sources = [source for source in (source, *others) if isinstance(source, LiftIndexMultiCastMixin)]

        # if 1 < len(lifted_sources):
        #     assert all(lifted_sources[0].lift_index == other.lift_index for other in lifted_sources[1:]), \
        #         f'layers do not match {[source.lift_index for source in lifted_sources]}'

        return source.merge(*others)

    return MultiCastOperator(op_func)


# def observe_on(scheduler: Scheduler = None):
#     """
#     Schedule elements emitted by the source on a dedicated scheduler
#     """
#
#     def op_func(source: MultiCast):
#         return source.observe_on(scheduler=scheduler)
#
#     return MultiCastOperator(op_func)


def share(
        func: Callable[[MultiCast], MultiCast],
):
    """
    Multi-cast the elements of the source to possibly multiple subscribers.

    This function is only valid when used inside a Multicast. Otherwise, it
    raise an exception.
    """

    stack = get_stack_lines()

    def op_func(source: MultiCast):
        return source.share_func(func=func, stack=stack)

    return MultiCastOperator(op_func)