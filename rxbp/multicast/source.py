from typing import Any, List, Callable, Union, Iterable

import rx
import rxbp
from rxbp.flowable import Flowable
from rxbp.flowables.subscribeonflowable import SubscribeOnFlowable
from rxbp.multicast.flowabledict import FlowableDict
from rxbp.multicast.multicast import MultiCast
from rxbp.multicast.multicastInfo import MultiCastInfo
from rxbp.multicast.multicastbase import MultiCastBase
from rxbp.multicast.rxextensions.debug_ import debug
from rxbp.multicast.singleflowablemixin import SingleFlowableMixin
from rxbp.torx import to_rx


def return_value(val: Any):
    class FromObjectMultiCast(MultiCastBase):
        def get_source(self, info: MultiCastInfo) -> rx.typing.Observable:
            return rx.return_value(val, scheduler=info.multicast_scheduler)

    return MultiCast(FromObjectMultiCast())


def from_iterable(vals: Iterable[Any]):
    class FromIterableMultiCast(MultiCastBase):
        def get_source(self, info: MultiCastInfo) -> Flowable:
            return rxbp.from_(vals)

    return MultiCast(FromIterableMultiCast())


def from_flowable(
        source: Flowable,
        key: Any = None,
        # func: Callable[[Flowable], MultiCastBase] = None,
):

    def selector(_, source):
        class SingleFlowableDict(SingleFlowableMixin, FlowableDict):
            def get_single_flowable(self) -> Flowable:
                return source

        return SingleFlowableDict(states={key: source})

    multicast = return_value(())

    if key is None:
        multicast = multicast.pipe(
            rxbp.multicast.op.share(lambda _: source, lambda _, source: source),
        )
    else:
        multicast = multicast.pipe(
            rxbp.multicast.op.share(lambda _: source, selector),
        )

    return multicast

    # if func is None:
    #     return multicast
    # else:
    #     class ToFlowableStream(MultiCastBase):
    #         def get_source(self, info: MultiCastInfo) -> Flowable:
    #             return multicast.source.map(lambda args: func(*args))
    #
    #     return MultiCast(ToFlowableStream())


def from_event(
        source: Flowable,
        func: Callable[[Any], MultiCastBase] = None,
):
    """ Emits `MultiCastBases` with `MultiCastBases` defined by a lift_func
    """

    class ReactOnMultiCast(MultiCastBase):
        def get_source(self, info: MultiCastInfo) -> Flowable:
            subscribe_on_flowable = Flowable(SubscribeOnFlowable(source, scheduler=info.source_scheduler))
            first_flowable = subscribe_on_flowable.pipe(
                rxbp.op.first(raise_exception=lambda f: f()),
            )

            if func is None:
                result = first_flowable
            else:
                result = first_flowable.pipe(
                    rxbp.op.map(selector=func),
                )

            return to_rx(result, subscribe_schduler=info.multicast_scheduler)

    return MultiCast(ReactOnMultiCast())
