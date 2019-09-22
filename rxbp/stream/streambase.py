from abc import ABC, abstractmethod
from functools import reduce
from typing import List, Callable, Any, Tuple, Dict

import rxbp
from rxbp.flowable import Flowable

from rxbp.stream.mapoperator import MapOperator
from rxbp.typing import BaseType


class StreamBase(ABC):
    # @abstractmethod
    # def par(self, ops: List[MapOperator], selector: Callable):
    #     ...
    #
    # @abstractmethod
    # def map(self, selector: Callable):
    #     ...
    #
    # @abstractmethod
    # def to_flowable(self):
    #     ...
    #
    # @abstractmethod
    # def run(self, func: Callable[[BaseType], Tuple[str, Flowable]]) -> Dict[str, Any]:
    #     ...

    # def __init__(self, source: Flowable[BaseType]):
    #     self.source = source

    # def par(self, ops: List[MapOperator], selector: Callable):
    #     def combine_func(val: BaseType):
    #         def gen_stream_op():
    #             for op in ops:
    #                 result = op(val)
    #                 yield result
    #
    #         return rxbp.zip(list(gen_stream_op()), lambda *v: selector(val, *v))
    #
    #     return FromFlowableStream(self.source.flat_map(combine_func))
    #
    # def map(self, selector: Callable):
    #     return FromFlowableStream(self.source.map(selector))

    def run(
            self,
            func: Callable[[BaseType], Tuple[str, Flowable]] = None,
    ) -> Dict[str, Any]:
        def default_func(base):
            print(base)
            return base.items()

        func = func or default_func

        def _create_dict(inner: BaseType):
            def gen_tuples():
                for name, flow in func(inner):
                    yield flow.to_list().map(lambda l, name=name: (name, l))

            return rxbp.zip(list(gen_tuples()), lambda *t2: dict(t2))

        return self.source.pipe(
            rxbp.op.flat_map(_create_dict),
        ).run()

    @property
    @abstractmethod
    def source(self) -> Flowable:
        ...
