from typing import List, Callable, Union, Dict, Any

from rxbp.flowable import Flowable
from rxbp.multicast.flowablestatemixin import FlowableStateMixin
from rxbp.multicast.multicastoperator import MultiCastOperator
from rxbp.multicast.multicastopmixin import MultiCastOpMixin
from rxbp.multicast.typing import MultiCastValue
from rxbp.typing import ValueType


def debug(name: str):
    def op_func(source: MultiCastOpMixin):
        return source.debug(name=name)

        # class DebugMultiCast(MultiCastBase):
        #     def get_source(self, info: MultiCastInfo) -> Flowable:
        #         print(f'{name}.get_source({info})')
        #
        #         return multicast.get_source(info=info).pipe(
        #             rx_debug(name),
        #         )
        #
        # return MultiCast(DebugMultiCast())

    return MultiCastOperator(op_func)


def defer(
        func: Callable[[MultiCastValue], MultiCastValue],
        initial: ValueType,
):
    def op_func(source: MultiCastOpMixin):
        return source.defer(func=func, initial=initial)
        # def lifted_func(multicast: MultiCastBase):
        #     return func(MultiCast(multicast))
        #
        # return MultiCast(DeferMultiCast(source=multi_cast, func=lifted_func, initial=initial))

    return MultiCastOperator(func=op_func)


def extend(
    func: Callable[[MultiCastValue], Union[Flowable, List, Dict, FlowableStateMixin]],
    # selector: Callable[[MultiCastValue, Flowable], MultiCastValue] = None,
):
    """ Shares a `Flowable` defined by a function `extend` that maps `MultiCast` value to a `Flowable`.
    A `selector` function is then used used extend the `MultiCast` value with the shared `Flowable`.

    :param func: a function that maps `MultiCast` value to a `Flowable`
    :param selector: a function that maps `MultiCast` value and the shared `Flowable` to some new `MultiCast` value
    """

    def op_func(source: MultiCastOpMixin):
        return source.extend(func=func)
        # return MultiCast(ExtendMultiCast(source=multicast, func=func))
    return MultiCastOperator(func=op_func)


def filter(
        func: Callable[[MultiCastValue], bool],
):
    """ Only emits those `MultiCast` values for which the given predicate hold.
    """

    def op_func(source: MultiCastOpMixin):
        return source.filter(func=func)
        # class FilterMultiCast(MultiCastBase):
        #     def get_source(self, info: MultiCastInfo) -> Flowable:
        #         source = multicast.get_source(info=info).pipe(
        #             rxop.filter(func)
        #         )
        #         return source
        #
        # return MultiCast(FilterMultiCast())
    return MultiCastOperator(op_func)


def flat_map(func: Callable[[MultiCastValue], MultiCastOpMixin]):
    """ Maps each `MultiCast` value by applying the given function `func` and flattens the result.
    """

    def op_func(source: MultiCastOpMixin):
        return source.flat_map(func=func)
        # class FlatMapMultiCast(MultiCastBase):
        #     def get_source(self, info: MultiCastInfo) -> rx.typing.Observable:
        #         return multicast.get_source(info=info).pipe(
        #             rxop.flat_map(lambda v: func(v).get_source(info=info)),
        #         )
        #
        # return MultiCast(FlatMapMultiCast())

    return MultiCastOperator(op_func)


def lift(
    func: Callable[[MultiCastOpMixin], MultiCastValue],
    # func: Callable[[MultiCast, MultiCastValue], MultiCastValue],
):
    """ Lift the current `MultiCast[T]` to a `MultiCast[MultiCast[T]]`.
    """

    def op_func(source: MultiCastOpMixin):
        return source.lift(func=func)

        # class LiftMultiCast(MultiCastBase):
        #     def get_source(self, info: MultiCastInfo) -> rx.typing.Observable:
        #         class InnerLiftMultiCast(MultiCastBase):
        #             def __init__(self, source: Flowable[MultiCastValue]):
        #                 self._source = source
        #
        #             def get_source(self, info: MultiCastInfo) -> Flowable:
        #                 return self._source
        #
        #         source = multi_cast.get_source(info=info).pipe(
        #             rxop.share(),
        #         )
        #
        #         inner_multicast = InnerLiftMultiCast(source=source)
        #         multicast_val = func(MultiCast(inner_multicast))
        #         return rx.return_value(multicast_val, scheduler=info.multicast_scheduler)
        # return LiftMultiCast()

    # def op_func(multi_cast: MultiCastBase):
    #     class LiftMultiCast(MultiCastBase):
    #         def get_source(self, info: MultiCastInfo) -> rx.typing.Observable:
    #             class InnerLiftMultiCast(MultiCastBase):
    #                 def __init__(self, source: rx.typing.Observable[MultiCastValue]):
    #                     self._source = source
    #
    #                 def get_source(self, info: MultiCastInfo) -> rx.typing.Observable:
    #                     return self._source
    #
    #             def lift_func(first: MultiCastValue, obs: rx.typing.Observable):
    #                 source = obs.pipe(
    #                     rxop.share(),
    #                 )
    #                 inner_multicast = InnerLiftMultiCast(source=source)
    #                 multicast_val = func(MultiCast(inner_multicast)) #, first)
    #                 return multicast_val.get_source(info=info)
    #
    #             return LiftObservable(
    #                 source=multi_cast.get_source(info=info),
    #                 func=lift_func,
    #                 subscribe_scheduler=info.multicast_scheduler,
    #             ).pipe(
    #                 rxop.map(lambda obs: MultiCast(InnerLiftMultiCast(obs)))
    #             )
    #
    #     return MultiCast(LiftMultiCast())

    return MultiCastOperator(op_func)


def merge(*others: MultiCastOpMixin):
    """ Merges two or more `MultiCast` streams together
    """

    def op_func(source: MultiCastOpMixin):
        return source.merge(*others)
        # class MergeMultiCast(MultiCastBase):
        #     def get_source(self, info: MultiCastInfo) -> rx.typing.Observable:
        #         multicasts = reversed([multicast] + list(others))
        #         return rx.merge(*[e.get_source(info=info) for e in multicasts])
        #
        # return MultiCast(MergeMultiCast())

    return MultiCastOperator(op_func)


def map(func: Callable[[MultiCastValue], MultiCastValue]):
    """ Maps each `MultiCast` value by applying the given function `func`
    """

    def op_func(source: MultiCastOpMixin):
        return source.map(func=func)
        # return MultiCast(MapMultiCast(source=multi_cast, func=func))

    return MultiCastOperator(op_func)


def reduce():
    """ Lift the current `MultiCast[ReducableMixin[T]]` to a `MultiCast[ReducableMixin[T]]`.
    """

    def op_func(source: MultiCastOpMixin):
        return source.reduce()
        # return MultiCast(ReduceMultiCast(source=source))

    return MultiCastOperator(op_func)


def share(
        func: Callable[[Any], MultiCastOpMixin],
):
    """ Splits the `MultiCast` stream in two, applies the given `MultiCast` operators on each of them, and merges the
    two streams together again.
    """

    def op_func(source: MultiCastOpMixin):
        shared_source = source.share()
        return func(shared_source)


        # class InnerMultiCast(MultiCastBase):
        #     def get_source(self, info: MultiCastInfo) -> rx.typing.Observable:
        #         shared_source = source.get_source(info=info).pipe(
        #             rxop.share(),
        #         )
        #
        #         class SharedMultiCast(MultiCastBase):
        #             def get_source(self, info: MultiCastInfo) -> rx.typing.Observable:
        #                 return shared_source
        #
        #         result = func(MultiCast(SharedMultiCast()))
        #
        #         return result.get_source(info=info)
        #
        # multicast = MultiCast(InnerMultiCast())
        # return multicast
    return MultiCastOperator(op_func)


# def share(
#         # func: Callable[[MultiCast], MultiCast],
# ):
#     """ Splits the `MultiCast` stream in two, applies the given `MultiCast` operators on each of them, and merges the
#     two streams together again.
#     """
#
#     def op_func(source: MultiCastOpMixin):
#         return source.share()
#
#     return MultiCastOperator(op_func)


def zip(
      *others: MultiCastOpMixin,
):
    #     """ Zips a set of `Flowables` together, which were selected by a `predicate`.
    #
    #     :param predicates: a list of functions that return True, if the current element is used for the zip operation
    #     :param selector: a function that maps the selected `Flowables` to some `MultiCast` value
    #     """

    def op_func(source: MultiCastOpMixin):
        return source.zip(*others)
        # return MultiCast(ZipMultiCast(sources=[multicast] + list(others)))

    return MultiCastOperator(op_func)


# def split(
#         left_ops: List[MultiCastOperator],
#         filter_left: Callable[[MultiCastValue], bool] = None,
#         filter_right: Callable[[MultiCastValue], bool] = None,
#         right_ops: List[MultiCastOperator] = None,
# ):
#     """ Splits the `MultiCast` stream in two, applies the given `MultiCast` operators on each of them, and merges the
#     two streams together again.
#
#             left
#             +----> op1 --> op2 -------------+
#            /                                 \
#     ------+------> op1 --> op2 --> op3 ------>+------>
#        extend  right                         merge
#
#     :param left_ops: `MultiCast` operators applied to the left
#     :param filter_left: (optional) a function that returns True, if the current element is passed left,
#     :param filter_right: (optional) a function that returns True, if the current element is passed right,
#     :param right_ops: (optional) `MultiCast` operators applied to the right
#     """
#
#     if filter_left is None:
#         filter_left = (lambda v: True)
#
#     if filter_right is None:
#         filter_right = (lambda v: True)
#
#     def op_func(source: MultiCast):
#         class SplitMultiCast(MultiCastBase):
#             def get_source(self, info: MultiCastInfo) -> rx.typing.Observable:
#                 shared_source = source.get_source(info=info).pipe(
#                     rxop.share(),
#                 )
#
#                 left_source = shared_source.pipe(
#                     rxop.filter(filter_left),
#                 )
#                 right_source = shared_source.pipe(
#                     rxop.filter(filter_right),
#                 )
#
#                 class LeftOpsStream(MultiCastBase):
#                     def get_source(self, info: MultiCastInfo) -> rx.typing.Observable:
#                         return left_source
#
#                 class RightOpsStream(MultiCastBase):
#                     def get_source(self, info: MultiCastInfo) -> rx.typing.Observable:
#                         return right_source
#
#                 left = MultiCast(LeftOpsStream()).pipe(*left_ops)
#
#                 if right_ops is None:
#                     right = RightOpsStream()
#                 else:
#                     right = MultiCast(RightOpsStream()).pipe(*right_ops)
#
#                 return left.get_source(info=info).pipe(
#                     rxop.merge(right.get_source(info=info))
#                 )
#
#         multicast = MultiCast(SplitMultiCast())
#         return multicast
#     return MultiCastOperator(op_func)

