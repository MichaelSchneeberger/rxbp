from typing import List, Callable, Any, Tuple, Dict

import rxbp
from rxbp.flowable import Flowable
from rxbp.flowablebase import FlowableBase
from rxbp.stream.deferstreamoperator import DeferType

from rxbp.stream.pipe import pipe
from rxbp.stream.streambase import StreamBase
from rxbp.stream.streamoperator import StreamOperator
from rxbp.typing import BaseType, ValueType


class Stream(StreamBase):
    def __init__(self, underlying: StreamBase):
        self.underlying = underlying

    def defer(
            self,
            func: Callable[[StreamBase], StreamBase],
            defer_sel: Callable[[StreamBase], DeferType],
            base_sel: Callable[[BaseType, DeferType], BaseType],
            initial: ValueType,
    ):

        def flat_map_func(base: BaseType):
            def defer_func(defer_flowable: Flowable):
                new_base = base_sel(base, defer_flowable)

                class FromObjectStream(StreamBase):
                    @property
                    def source(self) -> Flowable:
                        return rxbp.return_value(new_base)

                stream = FromObjectStream()
                result_stream = func(Stream(stream))

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

        class DeferStream(StreamBase):
            @property
            def source(self) -> Flowable:
                return source

        return Stream(DeferStream())

    def filter(
            self,
            func: Callable[[BaseType], bool],
    ) -> StreamBase:

        source = self.source.pipe(
            rxbp.op.filter(func)
        )

        class FilterStream(StreamBase):
            @property
            def source(self) -> Flowable:
                return source

        return Stream(FilterStream())

    def lift(
            self,
            lift_func: Callable[[Flowable[BaseType]], Flowable[BaseType]],
    ):
        def share_source(source: Flowable):
            lifted_base = lift_func(source)
            return lifted_base.pipe(
                rxbp.op.merge(rxbp.return_value(source))
            )

        source = self.source.pipe(
            rxbp.op.share(share_source)
        )

        class LiftStream(StreamBase):
            @property
            def source(self) -> Flowable:
                return source

        return Stream(LiftStream())

    def pipe_filtered(
            self,
            filter_func: Callable[[BaseType], bool],
            ops: List[StreamOperator],
    ) -> StreamBase:
        s1 = self.source.filter(filter_func)
        s2 = self.source.filter(lambda v: not filter_func(v))

        class UseFilterOpsStream(StreamBase):
            @property
            def source(self) -> Flowable:
                return s1

        stream = Stream(UseFilterOpsStream()).pipe(
            *ops
        )

        source = stream.source.merge(s2)

        class UseFilterStream(StreamBase):
            @property
            def source(self) -> Flowable:
                return source

        return Stream(UseFilterStream())

    def create_on_event(
            self,
            event_func: Callable[[BaseType], Flowable],
            create_func: Callable[[Any], BaseType],
    ):
        def share_func(shared_source: FlowableBase):
            return shared_source.pipe(
                rxbp.op.flat_map(event_func),
                rxbp.op.map(create_func),
                rxbp.op.merge(shared_source),
            )
        source = self.source.share(share_func)

        class ReactOnStream(StreamBase):
            @property
            def source(self) -> Flowable:
                return source

        return Stream(ReactOnStream())

    def share_and_extend(
            self,
            share_func: Callable[[BaseType], Flowable],
            extend_func: Callable[[BaseType, Flowable], BaseType]
    ):
        def func(base: BaseType):
            return share_func(base).pipe(
                rxbp.op.share(lambda f: rxbp.return_value(f)),
                rxbp.op.map(lambda f: extend_func(base, f)),
            )

        source = self.source.pipe(
            rxbp.op.flat_map(func),
        )

        class ShareAndExtendStream(StreamBase):
            @property
            def source(self) -> Flowable:
                return source

        return Stream(ShareAndExtendStream())

    def map(self, selector: Callable):
        source = self.source.map(selector)

        class MapStream(StreamBase):
            @property
            def source(self) -> Flowable:
                return source

        return Stream(MapStream())

    def pipe(self, *operators: StreamOperator):
        return Stream(pipe(*operators)(self))

    # def flat_map(
    #         self,
    #         func: Callable[[BaseType], Flowable[BaseType]],
    # ):
    #     def combine_func(val: BaseType) -> Flowable[BaseType]:
    #         return func(val)
    #
    #     return Stream(FromFlowableStream(self.source.flat_map(combine_func)))

    # def extend(self, ops: List[MapOperator], selector: Callable):
    #     # return self.underlying.par(ops=ops, selector=selector)
    #     def combine_func(val: BaseType):
    #         def gen_stream_op():
    #             for op in ops:
    #                 result = op(val)
    #                 yield result
    #
    #         return rxbp.zip(list(gen_stream_op()), lambda *v: selector(val, *v))
    #
    #     return Stream(FromFlowableStream(self.source.flat_map(combine_func)))

    def run(
            self,
            func: Callable[[Any], Tuple[str, Flowable]] = None,
    ) -> Dict[str, Any]:
        return self.underlying.run(func=func)

    @property
    def source(self):
        return self.underlying.source