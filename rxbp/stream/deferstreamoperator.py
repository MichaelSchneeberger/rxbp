from typing import Callable, Union, Tuple, TypeVar, Generic

import rxbp
from rxbp.flowable import Flowable

from rxbp.stream.streambase import StreamBase
from rxbp.typing import BaseType


DeferType = TypeVar('DeferType', bound=Union[Flowable, Tuple])


class DeferStreamOperator(Generic[DeferType]):
    def __init__(
            self,
            func: Callable[[StreamBase], StreamBase],
            defer_selector: Callable[[BaseType], DeferType],
            base_selector: Callable[[BaseType, DeferType], BaseType],
            initial: DeferType,
    ):
        self.func = func
        self.initial = initial
        self.defer_selector = defer_selector
        self.base_selector = base_selector

    def __call__(self, base: BaseType):
        def func(defer_flowable: Flowable):
            new_base = self.base_selector(base, defer_flowable)

            class FromObjectStream(StreamBase):
                @property
                def source(self) -> Flowable:
                    return rxbp.return_value(new_base)

            stream = FromObjectStream()
            result_stream = self.func(stream)

            return result_stream.to_flowable()

        def defer_selector(f: Flowable):
            def zip_if_necessary(inner: Flowable):
                result = self.defer_selector(inner)

                if isinstance(result, Flowable):
                    return result
                elif isinstance(result, list) or isinstance(result, tuple):
                    return rxbp.zip(list(result))
                else:
                    raise Exception('illegal type "{}"'.format(result))

            return f.flat_map(zip_if_necessary)

        return rxbp.defer(func=func, initial=self.initial, defer_selector=defer_selector)
