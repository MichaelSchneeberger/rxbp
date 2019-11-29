from typing import Any, List, Callable, Union, Iterable, Dict

import rx
import rxbp
from rxbp.flowable import Flowable
from rxbp.flowables.subscribeonflowable import SubscribeOnFlowable
from rxbp.multicast.flowabledict import FlowableDict
from rxbp.multicast.flowablestatemixin import FlowableStateMixin
from rxbp.multicast.multicast import MultiCast
from rxbp.multicast.multicastInfo import MultiCastInfo
from rxbp.multicast.multicastbase import MultiCastBase
from rxbp.multicast.rxextensions.debug_ import debug
from rxbp.multicast.singleflowablemixin import SingleFlowableMixin
from rxbp.torx import to_rx


def empty(is_list: bool = None, is_dict: bool = None):
    if is_list is True:
        init_val = []
    elif is_dict is True:
        init_val = {}
    else:
        init_val = {}

    class FromObjectMultiCast(MultiCastBase):
        def get_source(self, info: MultiCastInfo) -> rx.typing.Observable:
            return rx.return_value(init_val, scheduler=info.multicast_scheduler)

    return MultiCast(FromObjectMultiCast())


# def return_value(val: Any):
#     class FromObjectMultiCast(MultiCastBase):
#         def get_source(self, info: MultiCastInfo) -> rx.typing.Observable:
#             return rx.return_value(val, scheduler=info.multicast_scheduler)
#
#     return MultiCast(FromObjectMultiCast())


# def from_iterable(vals: Iterable[Any]):
#     class FromIterableMultiCast(MultiCastBase):
#         def get_source(self, info: MultiCastInfo) -> Flowable:
#             return rxbp.from_(vals)
#
#     return MultiCast(FromIterableMultiCast())


def from_flowable(
        *source: Union[Flowable, Dict[Any, Flowable], FlowableStateMixin],
):

    if len(source) == 0:
        return empty()

    first = source[0]

    if isinstance(first, Flowable):
        assert all(isinstance(s, Flowable) for s in source)

        multicast = empty(is_list=True)

        if len(source) == 1:
            append = lambda _, v: v
        else:
            append = lambda l, v: l + [s]

        for s in source:
            def for_func(s=s):
                return multicast.pipe(
                    rxbp.multicast.op.share(lambda _: s, append),
                )

            multicast = for_func()

    elif isinstance(first, dict):

        multicast = empty(is_dict=True)

        for key, s in first.items():
            def for_func(key=key, s=s):
                return multicast.pipe(
                    rxbp.multicast.op.share(lambda _: s, lambda d, source: {**d, key: source}),
                )

            multicast = for_func()

    elif isinstance(first, FlowableStateMixin):

        multicast = empty(is_dict=True)

        state = first.get_flowable_state()

        for key, s in state.items():
            def for_func(key=key, s=s):
                return multicast.pipe(
                    rxbp.multicast.op.share(lambda _: s, lambda d, source: first.set_flowable_state({**d, key: source})),
                )

            multicast = for_func()

    else:
        raise Exception(f'unexpected argument "{first}"')

    return multicast


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
