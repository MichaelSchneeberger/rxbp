from typing import List, Callable, Any, Dict, Tuple, Union

from rxbp.flowable import Flowable
from rxbp.stream.deferstreamoperator import DeferStreamOperator, DeferType
from rxbp.stream.streambase import StreamBase
from rxbp.stream.streamoperator import StreamOperator
from rxbp.typing import BaseType, ValueType
from rxbp.stream.stream import Stream


# def par(ops: List[MapOperator], selector: Callable[[BaseType], Flowable]):
#     def func(stream: StreamBase):
#         return stream.par(ops=ops, selector=selector)
#
#     return StreamOperator(func)


def pipe_filtered(
        filter_func: Callable[[BaseType], bool],
        ops: List[StreamOperator],
):
    def func(stream: Stream):
        return stream.pipe_filtered(filter_func=filter_func, ops=ops)
    return StreamOperator(func)


# def flat_map(func: Callable[[BaseType], Flowable[BaseType]]):
#     def op_func(stream: Stream):
#         return stream.flat_map(func=func)
#
#     return StreamOperator(op_func)


def filter(
        func: Callable[[BaseType], bool],
):
    def func(stream: Stream):
        return stream.filter(func=func)

    return StreamOperator(func)


def lift(
    lift_func: Callable[[Flowable[BaseType]], Flowable[BaseType]],
):
    def func(stream: Stream):
        return stream.lift(lift_func=lift_func)

    return StreamOperator(func)


def create_on_event(
    event_func: Callable[[BaseType], Flowable],
    create_func: Callable[[Any], BaseType],
):
    def func(stream: Stream):
        return stream.create_on_event(event_func=event_func, create_func=create_func)

    return StreamOperator(func)


def share_and_extend(
    share_func: Callable[[BaseType], Flowable],
    extend_func: Callable[[BaseType, Flowable], BaseType]
):
    def func(stream: Stream):
        return stream.share_and_extend(share_func=share_func, extend_func=extend_func)

    return StreamOperator(func)


def defer(
        func: Callable[[StreamBase], StreamBase],
        defer_sel: Callable[[StreamBase], DeferType],
        base_sel: Callable[[BaseType, DeferType], BaseType],
        initial: ValueType,
):
    def stream_op_func(stream: Stream):
        return stream.defer(func=func, defer_sel=defer_sel, base_sel=base_sel, initial=initial)

    return StreamOperator(func=stream_op_func)


def map(selector: Callable[[BaseType], BaseType]):
    def func(stream: StreamBase):
        return stream.map(selector=selector)

    return StreamOperator(func)


# def debug(name: str):
#     def func(stream: FromFlowableStream):
#
#
#         return stream.map(ops=[MapOperator(lambda )], selector=lambda v, _: v)
#
#     return StreamOperator(func)

