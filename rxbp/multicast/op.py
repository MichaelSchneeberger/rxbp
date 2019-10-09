from typing import List, Callable, Any

import rxbp
import rxbp.depricated

from rxbp.flowable import Flowable
from rxbp.multicast.multicastbase import MultiCastBase, MultiCastFlowable
from rxbp.multicast.multicastoperator import MultiCastOperator
from rxbp.multicast.typing import DeferType, MultiCastValue
from rxbp.typing import ValueType
from rxbp.multicast.multicast import MultiCast


def split(
        left_ops: List[MultiCastOperator],
        filter_left: Callable[[MultiCastValue], bool] = None,
        filter_right: Callable[[MultiCastValue], bool] = None,
        right_ops: List[MultiCastOperator] = None,
):
    """ Splits the `MultiCast` stream in two, applies the given `MultiCast` operators on each of them, and merges the
    two streams together again.

            left
            +----> op1 --> op2 -------------+
           /                                 \
    ------+------> op1 --> op2 --> op3 ------>+------>
       share  right                         merge

    :param left_ops: `MultiCast` operators applied to the left
    :param filter_left: (optional) a function that returns True, if the current element is passed left,
    :param filter_right: (optional) a function that returns True, if the current element is passed right,
    :param right_ops: (optional) `MultiCast` operators applied to the right
    """

    if filter_left is None:
        filter_left = (lambda v: True)

    if filter_right is None:
        filter_right = (lambda v: True)

    def op_func(multicast: MultiCast):
        class SplitMultiCast(MultiCastBase):
            def get_source(self, info: MultiCastBase.MultiCastInfo) -> Flowable:
                def share_source(source: Flowable):

                    left_source = source.filter(filter_left)
                    right_source = source.filter(filter_right)

                    class LeftOpsStream(MultiCastBase):
                        def get_source(self, info: MultiCastBase.MultiCastInfo) -> Flowable:
                            return left_source

                    class RightOpsStream(MultiCastBase):
                        def get_source(self, info: MultiCastBase.MultiCastInfo) -> Flowable:
                            return right_source

                    left = MultiCast(LeftOpsStream()).pipe(*left_ops)

                    if right_ops is None:
                        right = RightOpsStream()
                    else:
                        right = MultiCast(RightOpsStream()).pipe(*right_ops)

                    return left.get_source(info=info).pipe(
                        rxbp.op.merge(right.get_source(info=info)),
                    )

                source = multicast.get_source(info=info).share(share_source)
                return source

        return MultiCast(SplitMultiCast())
    return MultiCastOperator(op_func)


def zip(
    predicates: List[Callable[[MultiCastFlowable], bool]],
    selector: Callable[..., MultiCastValue],
):
    """ Zips a set of `Flowables` together, which were selected by a `predicate`.

    :param predicates: a list of functions that return True, if the current element is used for the zip operation
    :param selector: a function that maps the selected `Flowables` to some `MultiCast` value
    """

    def op_func(multicast: MultiCast, selector=selector):
        class ZipMultiCast(MultiCastBase):
            def get_source(self, info: MultiCastBase.MultiCastInfo) -> Flowable:
                def share_source(source: Flowable[MultiCastValue]):
                    def flat_map_func(v: MultiCastValue):
                        if isinstance(v, MultiCastBase.LiftedFlowable):
                            return v.source
                        elif isinstance(v, Flowable):
                            return v
                        else:
                            raise Exception(f'illegal case {v}')

                    flowables = [source.pipe(
                        rxbp.op.filter(lambda v: isinstance(v, MultiCastBase.LiftedFlowable) or isinstance(v, Flowable)),
                        rxbp.op.filter(predicate),
                        rxbp.op.flat_map(flat_map_func),
                        # rxbp.op.first(),
                    ) for predicate in predicates]

                    multicast = selector(*flowables)
                    return rxbp.return_value(multicast)

                source = multicast.get_source(info=info).share(share_source)
                return source

        return MultiCast(ZipMultiCast())

    return MultiCastOperator(func=op_func)


def filter(
        func: Callable[[MultiCastValue], bool],
):
    """ Only emits those `MultiCast` values for which the given predicate hold.
    """

    def op_func(multicast: MultiCast):


        class FilterMultiCast(MultiCastBase):
            def get_source(self, info: MultiCastBase.MultiCastInfo) -> Flowable:
                source = multicast.get_source(info=info).pipe(
                    rxbp.op.filter(func)
                )
                return source

        return MultiCast(FilterMultiCast())
    return MultiCastOperator(op_func)


def lift(
    func: Callable[[MultiCast], MultiCastValue],
):
    """ Lift the current `MultiCast[T]` to a `MultiCast[MultiCast[T]]`.
    """

    def op_func(multi_cast: MultiCastBase):
        class LiftMultiCast(MultiCastBase):
            def get_source(self, info: MultiCastBase.MultiCastInfo) -> Flowable:
                class InnerLiftMultiCast(MultiCastBase):
                    def __init__(self, source: Flowable[MultiCastValue]):
                        self._source = source

                    def get_source(self, info: MultiCastBase.MultiCastInfo) -> Flowable:
                        return self._source

                inner_multicast = InnerLiftMultiCast(source=multi_cast.get_source(info=info))
                multicast_val = func(MultiCast(inner_multicast))
                return rxbp.return_value(multicast_val)

        return MultiCast(LiftMultiCast())

    return MultiCastOperator(op_func)


def share(
    func: Callable[[MultiCastValue], Flowable],
    selector: Callable[[MultiCastValue, Flowable], MultiCastValue] = None,
):
    """ Shares a `Flowable` defined by a function `share` that maps `MultiCast` value to a `Flowable`.
    A `selector` function is then used used extend the `MultiCast` value with the shared `Flowable`.

    :param func: a function that maps `MultiCast` value to a `Flowable`
    :param selector: a function that maps `MultiCast` value and the shared `Flowable` to some new `MultiCast` value
    """

    def op_func(multicast: MultiCast):
        class ShareAndMapMultiCast(MultiCastBase):
            def get_source(self, info: MultiCastBase.MultiCastInfo) -> Flowable:
                if selector is None:
                    selector_ = lambda _, v: v
                else:
                    selector_ = selector

                def flat_map_func(base: MultiCastValue):
                    return func(base).pipe(
                        rxbp.depricated.share(lambda f: rxbp.return_value(f)),
                        rxbp.op.map(lambda f: selector_(base, f)),
                    )

                source = multicast.get_source(info=info).pipe(
                    rxbp.op.flat_map(flat_map_func),
                )

                return source

        return MultiCast(ShareAndMapMultiCast())
    return MultiCastOperator(func=op_func)


def defer(
        func: Callable[[MultiCastValue], MultiCastValue],
        defer_sel: Callable[[MultiCastValue], DeferType],
        base_sel: Callable[[MultiCastValue, DeferType], MultiCastValue],
        initial: ValueType,
):
    def stream_op_func(multi_cast: MultiCast):
        class DeferMultiCast(MultiCastBase):
            def get_source(self, info: MultiCastBase.MultiCastInfo) -> Flowable:
                def flat_map_func(base: MultiCastBase):
                    def defer_func(defer_flowable: Flowable):
                        new_base = base_sel(base, defer_flowable)

                        class FromObjectStream(MultiCastBase):
                            @property
                            def source(self) -> Flowable:
                                return rxbp.return_value(new_base)

                        stream = FromObjectStream()
                        result_stream = func(MultiCast(stream))

                        return result_stream.source

                    def defer_selector(f: Flowable):
                        def zip_if_necessary(inner: Flowable):
                            result = defer_sel(inner)

                            if isinstance(result, Flowable):
                                return result
                            elif isinstance(result, list) or isinstance(result, tuple):
                                return rxbp.zip(list(result))
                            else:
                                raise Exception('illegal type "{}"'.format(result))

                        return f.pipe(
                            rxbp.op.flat_map(zip_if_necessary),
                        )

                    return rxbp.defer(func=defer_func, initial=initial, defer_selector=defer_selector)

                source = multi_cast.get_source(info=info).flat_map(flat_map_func)
                return source

        return MultiCast(DeferMultiCast())

    return MultiCastOperator(func=stream_op_func)


def merge(*others: MultiCast):
    """ Merges two or more `MultiCast` streams together
    """

    def op_func(multicast: MultiCast):
        source = multicast.source

        class MergeMultiCast(MultiCastBase):
            def get_source(self, info: MultiCastBase.MultiCastInfo) -> Flowable:
                return source.merge(*[e.get_source(info=info) for e in others])

        return MultiCast(MergeMultiCast())

    return MultiCastOperator(op_func)


def map(func: Callable[[MultiCastValue], MultiCastValue]):
    """ Maps each `MultiCast` value by applying the given function `func`
    """

    def op_func(multi_cast: MultiCast):
        class MapMultiCast(MultiCastBase):
            def get_source(self, info: MultiCastBase.MultiCastInfo) -> Flowable:
                return multi_cast.get_source(info=info).pipe(
                    rxbp.op.map(func),
                )

        return MultiCast(MapMultiCast())

    return MultiCastOperator(op_func)


def flat_map(func: Callable[[MultiCastValue], MultiCast[MultiCastValue]]):
    """ Maps each `MultiCast` value by applying the given function `func` and flattens the result.
    """

    def op_func(multicast: MultiCast):
        class FlatMapMultiCast(MultiCastBase):
            def get_source(self, info: MultiCastBase.MultiCastInfo) -> Flowable:
                return multicast.get_source(info=info).pipe(
                    rxbp.op.flat_map(lambda v: func(v).get_source(info=info)),
                )

        return MultiCast(FlatMapMultiCast())

    return MultiCastOperator(op_func)


def debug(name: str):
    def func(multicast: MultiCast):
        class DebugMultiCast(MultiCastBase):
            def get_source(self, info: MultiCastBase.MultiCastInfo) -> Flowable:
                return multicast.get_source(info=info).pipe(
                    rxbp.op.debug(name),
                )

        return MultiCast(DebugMultiCast())

    return MultiCastOperator(func)
