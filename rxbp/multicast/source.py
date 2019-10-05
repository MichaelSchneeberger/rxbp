from typing import Any, List, Callable, Union, Iterable

import rxbp
from rxbp.flowable import Flowable
from rxbp.multicast.multicast import MultiCast
from rxbp.multicast.multicastbase import MultiCastBase


def return_value(val: Any):
    class FromObjectMultiCast(MultiCastBase):
        def get_source(self, info: MultiCastBase.MultiCastInfo) -> Flowable:
            return rxbp.return_value(val)

    return MultiCast(FromObjectMultiCast())


def from_iterable(vals: Iterable[Any]):
    class FromIterableMultiCast(MultiCastBase):
        def get_source(self, info: MultiCastBase.MultiCastInfo) -> Flowable:
            return rxbp.from_(vals)

    return MultiCast(FromIterableMultiCast())


def from_flowable(
        source: Flowable,
        func: Callable[[Flowable], MultiCastBase] = None,
):
    multicast = return_value(())

    multicast = multicast.pipe(
        rxbp.multicast.op.share(lambda _: source, lambda _, flowable: flowable),
    )

    if func is None:
        return multicast
    else:
        class ToFlowableStream(MultiCastBase):
            def get_source(self, info: MultiCastBase.MultiCastInfo) -> Flowable:
                return multicast.source.map(lambda args: func(*args))

        return MultiCast(ToFlowableStream())


def from_event(
        source: Flowable,
        func: Callable[[Any], MultiCastBase] = None,
):
    """ Emits `MultiCastBases` with `MultiCastBases` defined by a lift_func
    """

    class ReactOnMultiCast(MultiCastBase):
        def get_source(self, info: MultiCastBase.MultiCastInfo) -> Flowable:
            source_ = source.pipe(
                rxbp.op.subscribe_on(scheduler=info.subscribe_scheduler),
                rxbp.op.first(raise_exception=lambda f: f()),
            )

            if func is None:
                return source_
            else:
                return source_.pipe(
                    rxbp.op.map(selector=func),
                )

    return MultiCast(ReactOnMultiCast())
