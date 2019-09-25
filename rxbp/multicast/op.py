from typing import List, Callable, Any

from rxbp.flowable import Flowable
from rxbp.multicast.multicastoperator import MultiCastOperator
from rxbp.multicast.typing import DeferType, MultiCastValue
from rxbp.typing import ValueType
from rxbp.multicast.multicast import MultiCast


def split(
        sel_left: Callable[[MultiCastValue], bool],
        left_ops: List[MultiCastOperator] = None,
        right_ops: List[MultiCastOperator] = None,
):
    """ Applies the pipe with only a subset of `MultiCastBases` and emits the resulting `MultiCastBases` with
    the other `MultiCastBases`.

        `MultiCastBases` where predicate didn't hold
            +-------------------------------+
           /                                 \
    ------+------> op1 --> op2 --> op3 ------>+------>
        filter                              merge

    """

    def func(stream: MultiCast):
        return stream.split(sel_left=sel_left, left_ops=left_ops, right_ops=right_ops)
    return MultiCastOperator(func)


def filter(
        func: Callable[[MultiCastValue], bool],
):
    """ Only emits those `MultiCastBases` for which the given predicate holds.
    """

    def func(stream: MultiCast):
        return stream.filter(func=func)

    return MultiCastOperator(func)


def lift(
    # filter_func: Callable[[MultiCast], MultiCast],
    # func2: Callable[[MultiCast], MultiCastValue],
    func: Callable[[MultiCast], MultiCastValue],
):
    """ Lifting
    """

    def op_func(stream: MultiCast):
        multi_cast = stream.lift(func=func)
        return MultiCastOperator.ReturnValue(multi_cast=multi_cast, n_lifts=1)

    return MultiCastOperator(op_func)


def create_on_event(
    event_func: Callable[[MultiCastValue], Flowable[Any]],
    create_func: Callable[[Any], MultiCastValue],
):
    """ Emits `MultiCastBases` with `MultiCastBases` defined by a lift_func
    """

    def func(stream: MultiCast):
        return stream.create_on_event(event_func=event_func, create_func=create_func)

    return MultiCastOperator(func)


def share(
    func: Callable[[MultiCastValue], Flowable],
    selector: Callable[[MultiCastValue, Flowable], MultiCastValue] = None,
):
    def op_func(stream: MultiCast) -> MultiCastOperator.ReturnValue:
        multi_cast = stream.share(func=func, selector=selector)
        return MultiCastOperator.ReturnValue(multi_cast=multi_cast)

    return MultiCastOperator(func=op_func)


def defer(
        func: Callable[[MultiCastValue], MultiCastValue],
        defer_sel: Callable[[MultiCastValue], DeferType],
        base_sel: Callable[[MultiCastValue, DeferType], MultiCastValue],
        initial: ValueType,
):
    def stream_op_func(stream: MultiCast):
        return stream.defer(func=func, defer_sel=defer_sel, base_sel=base_sel, initial=initial)

    return MultiCastOperator(func=stream_op_func)


def merge(*others: MultiCast):
    def func(stream: MultiCast):
        return MultiCastOperator.ReturnValue(stream.merge(*others))

    return MultiCastOperator(func)


def map(func: Callable[[MultiCastValue], MultiCastValue]):
    def op_func(stream: MultiCast):
        return MultiCastOperator.ReturnValue(stream.map(func=func))

    return MultiCastOperator(op_func)


def flat_map(func: Callable[[MultiCastValue], MultiCast[MultiCastValue]]):
    def op_func(stream: MultiCast):
        return MultiCastOperator.ReturnValue(stream.flat_map(func=func))

    return MultiCastOperator(op_func)



# def debug(name: str):
#     def func(stream: FromFlowableStream):
#
#
#         return stream.map(ops=[MapOperator(lambda )], selector=lambda v, _: v)
#
#     return StreamOperator(func)

