from typing import List, Callable, Any, Tuple, Dict, Generic

import rxbp
from rxbp.flowable import Flowable
from rxbp.flowablebase import FlowableBase
from rxbp.flowableoperator import FlowableOperator
from rxbp.flowables.cacheservefirstflowable import CacheServeFirstFlowable
from rxbp.flowables.delaysubscriptionflowable import DelaySubscriptionFlowable
from rxbp.flowables.refcountflowable import RefCountFlowable

from rxbp.multicast.pipe import pipe
from rxbp.multicast.multicastbase import MultiCastBase
from rxbp.multicast.multicastoperator import MultiCastOperator
from rxbp.multicast.typing import DeferType, MultiCastValue
from rxbp.typing import ValueType


class MultiCast(MultiCastBase, Generic[MultiCastValue]):
    def __init__(self, underlying: MultiCastBase):
        self.underlying = underlying

    def _share(
            self,
            func: Callable[[Flowable], Flowable],
    ):
        def inner_func(source: Flowable) -> Flowable:
            def lifted_func(f: RefCountFlowable):
                result = func(Flowable(f))
                return result

            flowable = CacheServeFirstFlowable(source=source, func=lifted_func)
            return Flowable(flowable)

        return FlowableOperator(inner_func)

    def defer(
            self,
            func: Callable[[MultiCastBase], MultiCastBase],
            defer_sel: Callable[[MultiCastBase], DeferType],
            base_sel: Callable[[MultiCastBase, DeferType], MultiCastBase],
            initial: ValueType,
    ):

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

        source = self.source.flat_map(flat_map_func)

        class DeferMultiCast(MultiCastBase):
            @property
            def source(self) -> Flowable:
                return source

        return MultiCast(DeferMultiCast())

    # def filter(
    #         self,
    #         func: Callable[[MultiCastBase], bool],
    # ) -> MultiCastBase:
    #
    #     source = self.source.pipe(
    #         rxbp.op.filter(func)
    #     )
    #
    #     class FilterMultiCast(MultiCastBase):
    #         @property
    #         def source(self) -> Flowable:
    #             return source
    #
    #     return MultiCast(FilterMultiCast())

    # def lift(
    #         self,
    #         # filter_func: Callable[['MultiCast'], 'MultiCast'],
    #         # func2: Callable[['MultiCast'], MultiCastBase],
    #         func: Callable[['MultiCast'], MultiCastValue],
    # ):
    #     this_source = self.source
    #
    #     # class SharedMultiCast(MultiCastBase):
    #     #     @property
    #     #     def source(self) -> Flowable:
    #     #         return this_source
    #     #
    #     # multicast = MultiCast(SharedMultiCast())
    #     # multicast_val = func(multicast)
    #     # source = rxbp.return_value(multicast_val)
    #
    #     def share_source(source: Flowable[MultiCastBase]):
    #         class SharedMultiCast(MultiCastBase):
    #             @property
    #             def source(self) -> Flowable:
    #                 return source
    #
    #         multicast = MultiCast(SharedMultiCast())
    #         multicast_val = func(multicast)
    #         source = rxbp.return_value(multicast_val)
    #
    #         multicast = MultiCast(SharedMultiCast())
    #         multi_cast_val = func(multicast)
    #
    #         delayed_source = DelaySubscriptionFlowable(source=source)
    #
    #         return rxbp.return_value(multi_cast_val).pipe(
    #             rxbp.op.merge(delayed_source)
    #         )
    #
    #     source = self.source.pipe(
    #         self._share(share_source)
    #     )
    #
    #     class LiftSubscriptionMultiCast(MultiCastBase):
    #         @property
    #         def source(self) -> Flowable:
    #             return source
    #
    #     return MultiCast(LiftSubscriptionMultiCast())

    # def split(
    #         self,
    #         sel_left: Callable[[MultiCastValue], bool],
    #         left_ops: List[MultiCastOperator] = None,
    #         right_ops: List[MultiCastOperator] = None,
    # ) -> MultiCastBase:
    #     # for op in ops:
    #     #     assert isinstance(op, MultiCastOperator), f'operator "{op}" cannot be used with `pipe_filtered`'
    #
    #     def share_source(source: Flowable):
    #
    #         left_source = source.filter(sel_left)
    #         right_source = source.filter(lambda v: not sel_left(v))
    #
    #         class LeftOpsStream(MultiCastBase):
    #             @property
    #             def source(self) -> Flowable:
    #                 return left_source
    #
    #         class RightOpsStream(MultiCastBase):
    #             @property
    #             def source(self) -> Flowable:
    #                 return right_source
    #
    #         left_return = pipe(*left_ops)(MultiCast(LeftOpsStream()))
    #         right_return = pipe(*right_ops)(MultiCast(RightOpsStream()))
    #
    #         delta_n_lifts = left_return.n_lifts - right_return.n_lifts
    #
    #         left = left_return.multi_cast
    #         right = right_return.multi_cast
    #
    #         if 0 < delta_n_lifts:
    #             for _ in range(delta_n_lifts):
    #                 right = DelaySubscriptionFlowable(right)
    #         elif delta_n_lifts < 0:
    #             for _ in range(-delta_n_lifts):
    #                 left = DelaySubscriptionFlowable(left)
    #
    #         left.source.pipe(
    #             rxbp.op.merge(right.source),
    #         )
    #
    #     # return_val = MultiCast(UseFilterOpsStream()).pipe(
    #     #     *ops
    #     # )
    #
    #     # for _ in range(return_val.n_lifts):
    #     #     s2 = rxbp.return_value(s2)
    #
    #     # source = return_val.multi_cast.source.merge(s2)
    #
    #     source = self.source.share(share_source)
    #
    #     class UseFilterMultiCast(MultiCastBase):
    #         @property
    #         def source(self) -> Flowable:
    #             return source
    #
    #     return MultiCast(UseFilterMultiCast())

    # def create_on_event(
    #         self,
    #         event_func_list: List[Callable[[MultiCastBase], Flowable[Any]]],
    #         create_func: Callable[[Any], MultiCastBase],
    # ):
    #     def share_func(shared_source: FlowableBase):
    #         def gen_events():
    #             for event_func in event_func_list:
    #                 yield shared_source.pipe(
    #                     rxbp.op.flat_map(event_func),
    #                 )
    #
    #         return rxbp.merge(*gen_events()).pipe(
    #
    #         )
    #         # return shared_source.pipe(
    #         #     rxbp.op.flat_map(event_func),
    #         #     rxbp.op.map(create_func),
    #         #     rxbp.op.merge(shared_source),
    #         # )
    #     source = self.source.share(share_func)
    #
    #     class ReactOnMultiCast(MultiCastBase):
    #         @property
    #         def source(self) -> Flowable:
    #             return source
    #
    #     return MultiCast(ReactOnMultiCast())

    # def share(
    #         self,
    #         func: Callable[[MultiCastValue], Flowable],
    #         selector: Callable[[MultiCastValue, Flowable], MultiCastValue] = None,
    # ):
    #
    #     if selector is None:
    #         selector = lambda _, v: v
    #
    #     def flat_map_func(base: MultiCastValue):
    #         return func(base).pipe(
    #             self._share(lambda f: rxbp.return_value(f)),
    #             rxbp.op.map(lambda f: selector(base, f)),
    #         )
    #
    #     source = self.source.pipe(
    #         rxbp.op.flat_map(flat_map_func),
    #     )
    #
    #     class ShareAndMapMultiCast(MultiCastBase):
    #         @property
    #         def source(self) -> Flowable:
    #             return source
    #
    #     return MultiCast(ShareAndMapMultiCast())

    # def merge(self, *others: MultiCastBase):
    #     source = self.source
    #
    #     class MergeMultiCast(MultiCastBase):
    #         @property
    #         def source(self) -> Flowable:
    #             return source.merge(*[e.source for e in others])
    #
    #     return MultiCast(MergeMultiCast())

    # def flat_map(self, func: Callable[[MultiCastValue], 'MultiCast[MultiCastValue]']):
    #     source = self.source
    #
    #     class FlatMapMultiCast(MultiCastBase):
    #         @property
    #         def source(self) -> Flowable:
    #             return source.pipe(
    #                 rxbp.op.flat_map(lambda v: func(v).source),
    #             )
    #
    #     return MultiCast(FlatMapMultiCast())

    # def map(self, func: Callable[[MultiCastValue], MultiCastValue]):
    #     source = self.source
    #
    #     class MapMultiCast(MultiCastBase):
    #         @property
    #         def source(self) -> Flowable:
    #             return source.pipe(
    #                 rxbp.op.map(func),
    #             )
    #
    #     return MultiCast(MapMultiCast())

    def pipe(self, *operators: MultiCastOperator):
        return pipe(*operators)(self)

    @property
    def source(self):
        return self.underlying.source