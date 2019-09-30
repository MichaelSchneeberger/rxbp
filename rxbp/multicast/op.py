from typing import List, Callable, Any

import rxbp

from rxbp.flowable import Flowable
from rxbp.multicast.multicastbase import MultiCastBase
from rxbp.multicast.multicastoperator import MultiCastOperator
from rxbp.multicast.typing import DeferType, MultiCastValue
from rxbp.typing import ValueType
from rxbp.multicast.multicast import MultiCast


def split(
        left_ops: List[MultiCastOperator],
        sel_left: Callable[[MultiCastValue], bool] = None,
        sel_right: Callable[[MultiCastValue], bool] = None,
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

    if sel_left is None:
        sel_left = (lambda v: True)

    if sel_right is None:
        sel_right = (lambda v: True)

    def op_func(multicast: MultiCast):
        def share_source(source: Flowable):

            left_source = source.filter(sel_left)
            right_source = source.filter(sel_right)

            class LeftOpsStream(MultiCastBase):
                @property
                def source(self) -> Flowable:
                    return left_source

            class RightOpsStream(MultiCastBase):
                @property
                def source(self) -> Flowable:
                    return right_source

            left = MultiCast(LeftOpsStream()).pipe(*left_ops)

            if right_ops is None:
                right = RightOpsStream()
            else:
                right = MultiCast(RightOpsStream()).pipe(*right_ops)

            # left = pipe(*left_ops)(MultiCast(LeftOpsStream()))
            # right = pipe(*right_ops)(MultiCast(RightOpsStream()))

            return left.source.pipe(
                # rxbp.op.debug('left'),
                rxbp.op.merge(right.source.pipe(
                    # rxbp.op.debug('right'),
                )),
                # rxbp.op.debug('out'),
            )

        source = multicast.source.share(share_source)

        class SplitMultiCast(MultiCastBase):
            @property
            def source(self) -> Flowable:
                return source

        return MultiCast(SplitMultiCast())
    return MultiCastOperator(op_func)



def zip(
    predicates: List[Callable[[MultiCastValue], bool]],
    selector: Callable[..., MultiCastValue],
):
    def op_func(multicast: MultiCast, selector=selector):
        def share_source(source: Flowable[MultiCastValue]):
            flowables = [source.filter(predicate).flat_map(lambda v: v.source) for predicate in predicates]
            multicast = selector(*flowables)
            return rxbp.return_value(multicast)

        source = multicast.source.share(share_source)

        class ZipMultiCast(MultiCastBase):
            @property
            def source(self) -> Flowable:
                return source

        return MultiCast(ZipMultiCast())

    return MultiCastOperator(func=op_func)


def filter(
        func: Callable[[MultiCastValue], bool],
):
    """ Only emits those `MultiCastBases` for which the given predicate holds.
    """

    def op_func(multicast: MultiCast):
        source = multicast.source.pipe(
            rxbp.op.filter(func)
        )

        class FilterMultiCast(MultiCastBase):
            @property
            def source(self) -> Flowable:
                return source

        return MultiCast(FilterMultiCast())

    return MultiCastOperator(op_func)


def lift(
    func: Callable[[MultiCast], MultiCastValue],
):
    """ Lifting
    """

    def op_func(multi_cast: MultiCastBase):
        class LiftMultiCast(MultiCastBase):
            @property
            def source(self):

                class InnerLiftMultiCast(MultiCastBase):
                    def __init__(self, source: Flowable[MultiCastValue]):
                        self._source = source

                    @property
                    def source(self):
                        return self._source

                inner_multicast = InnerLiftMultiCast(source=multi_cast.source)
                multicast_val = func(MultiCast(inner_multicast))
                return rxbp.return_value(multicast_val)

        return MultiCast(LiftMultiCast())

    return MultiCastOperator(op_func)


# def create_on_event(
#     event_func: Callable[[MultiCastValue], Flowable[Any]],
#     create_func: Callable[[Any], MultiCastValue],
# ):
#     """ Emits `MultiCastBases` with `MultiCastBases` defined by a lift_func
#     """
#
#     def func(stream: MultiCast):
#         return stream.create_on_event(event_func=event_func, create_func=create_func)
#
#     return MultiCastOperator(func)


def share(
    func: Callable[[MultiCastValue], Flowable],
    selector: Callable[[MultiCastValue, Flowable], MultiCastValue] = None,
):
    def op_func(multicast: MultiCast, selector=selector):
        if selector is None:
            selector = lambda _, v: v

        def flat_map_func(base: MultiCastValue):
            return func(base).pipe(
                rxbp.op.share(lambda f: rxbp.return_value(f)),
                rxbp.op.map(lambda f: selector(base, f)),
            )

        source = multicast.source.pipe(
            rxbp.op.flat_map(flat_map_func),
        )

        class ShareAndMapMultiCast(MultiCastBase):
            @property
            def source(self) -> Flowable:
                return source

        return MultiCast(ShareAndMapMultiCast())

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
    def op_func(multicast: MultiCast):
        source = multicast.source

        class MergeMultiCast(MultiCastBase):
            @property
            def source(self) -> Flowable:
                return source.merge(*[e.source for e in others])

        return MultiCast(MergeMultiCast())

    return MultiCastOperator(op_func)


def map(func: Callable[[MultiCastValue], MultiCastValue]):
    def op_func(multi_cast: MultiCast):
        class MapMultiCast(MultiCastBase):
            @property
            def source(self) -> Flowable:
                return multi_cast.source.pipe(
                    rxbp.op.map(func),
                )

        return MultiCast(MapMultiCast())

    return MultiCastOperator(op_func)


def flat_map(func: Callable[[MultiCastValue], MultiCast[MultiCastValue]]):
    def op_func(multicast: MultiCast):
        source = multicast.source

        class FlatMapMultiCast(MultiCastBase):
            @property
            def source(self) -> Flowable:
                return source.pipe(
                    rxbp.op.flat_map(lambda v: func(v).source),
                )

        return MultiCast(FlatMapMultiCast())

    return MultiCastOperator(op_func)



def debug(name: str):
    def func(multicast: MultiCast):
        class DebugMultiCast(MultiCastBase):
            @property
            def source(self) -> Flowable:
                return multicast.source.pipe(
                    rxbp.op.debug(name),
                )

        return MultiCast(DebugMultiCast())

    return MultiCastOperator(func)

