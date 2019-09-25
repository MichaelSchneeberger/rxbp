from typing import Any, List, Callable, Union, Iterable

import rxbp
from rxbp.flowable import Flowable
from rxbp.multicast.multicast import MultiCast
from rxbp.multicast.multicastbase import MultiCastBase


def return_value(val: Any):
    class FromObjectMultiCast(MultiCastBase):
        @property
        def source(self) -> Flowable:
            return rxbp.return_value(val)

    return MultiCast(FromObjectMultiCast())


def from_iterable(vals: Iterable[Any]):
    class FromIterableMultiCast(MultiCastBase):
        @property
        def source(self) -> Flowable:
            return rxbp.from_(vals)

    return MultiCast(FromIterableMultiCast())


def from_flowables(
        source: Flowable,
        func: Callable[[Flowable], MultiCastBase] = None,
):
    multicast = return_value(())

    # for source in sources:
    #     multicast = multicast.pipe(
    #         rxbp.multicast.op.share(lambda _: source, lambda base, flowable: base + (flowable,)),
    #     )

    multicast = multicast.pipe(
        rxbp.multicast.op.share(lambda _: source, lambda _, flowable: flowable),
    )

    if func is None:
        return multicast
    else:
        class ToFlowableStream(MultiCastBase):
            @property
            def source(self) -> Flowable:
                return multicast.source.map(lambda args: func(*args))

        return MultiCast(ToFlowableStream())
