from typing import Any, Callable, Union, Dict, Iterable

import rx

import rxbp
from rxbp.flowable import Flowable
from rxbp.flowables.refcountflowable import RefCountFlowable
from rxbp.flowables.subscribeonflowable import SubscribeOnFlowable
from rxbp.multicast.flowablestatemixin import FlowableStateMixin
from rxbp.multicast.multicast import MultiCast
from rxbp.multicast.multicastInfo import MultiCastInfo
from rxbp.multicast.multicastbase import MultiCastBase
from rxbp.multicast.multicastflowable import MultiCastFlowable
from rxbp.multicast.op import merge as merge_op
from rxbp.torx import to_rx


def empty():
    class EmptyMultiCast(MultiCastBase):
        def get_source(self, info: MultiCastInfo) -> rx.typing.Observable:
            return rx.empty(scheduler=info.multicast_scheduler)

    return MultiCast(EmptyMultiCast())


def return_value(val: Any):
    class FromReturnValueMultiCast(MultiCastBase):
        def get_source(self, info: MultiCastInfo) -> rx.typing.Observable:
            return rx.return_value(val, scheduler=info.multicast_scheduler)

    return MultiCast(FromReturnValueMultiCast())


def from_iterable(vals: Iterable[Any]):
    class FromIterableMultiCast(MultiCastBase):
        def get_source(self, info: MultiCastInfo) -> Flowable:
            return rx.from_(vals, scheduler=info.multicast_scheduler)

    return MultiCast(FromIterableMultiCast())


def from_observable(val: rx.typing.Observable):
    class FromObservableMultiCast(MultiCastBase):
        def get_source(self, info: MultiCastInfo) -> Flowable:
            return val

    return MultiCast(FromObservableMultiCast())


def from_flowable(
        *source: Union[Flowable, Dict[Any, Flowable], FlowableStateMixin],
):

    if len(source) == 0:
        return empty()

    first = source[0]

    def to_multi_cast_flowable(f: Flowable):
        assert isinstance(f, Flowable), f'{f} is not a Flowable'
        assert not isinstance(f, MultiCastFlowable), "it's not allowed to multicast a Multicast Flowable"

        return MultiCastFlowable(RefCountFlowable(f))

    if isinstance(first, Flowable):
        if len(source) == 1:
            init_val = to_multi_cast_flowable(first)
        else:
            init_val = [to_multi_cast_flowable(s) for s in source]

    elif isinstance(first, list):
        init_val = [to_multi_cast_flowable(s) for s in first]

    elif isinstance(first, dict):
        init_val = {key: to_multi_cast_flowable(s) for key, s in first.items()}

    elif isinstance(first, FlowableStateMixin):
        state = first.get_flowable_state()
        init_val = {key: to_multi_cast_flowable(s) for key, s in state.items()}

    else:
        raise Exception(f'unexpected argument "{first}"')

    class FromObjectMultiCast(MultiCastBase):
        def get_source(self, info: MultiCastInfo) -> rx.typing.Observable:
            # first element has to be scheduled on dedicated scheduler. Unlike in rxbp,
            # this "subscribe scheduler" is not automatically provided in rx, that is
            # why it must be provided as the `scheduler` argument of the `return_value`
            # operator.
            return rx.return_value(init_val, scheduler=info.multicast_scheduler)

    return MultiCast(FromObjectMultiCast())


# def from_event(
#         source: Flowable,
#         func: Callable[[Any], MultiCastBase] = None,
# ):
#     """ Emits `MultiCastBases` with `MultiCastBases` defined by a lift_func
#     """
#
#     class FromEventMultiCast(MultiCastBase):
#         def get_source(self, info: MultiCastInfo) -> Flowable:
#             subscribe_on_flowable = Flowable(SubscribeOnFlowable(source, scheduler=info.source_scheduler))
#             first_flowable = subscribe_on_flowable.pipe(
#                 rxbp.op.first(raise_exception=lambda f: f()),
#             )
#
#             if func is None:
#                 result = first_flowable
#             else:
#                 result = first_flowable.pipe(
#                     rxbp.op.map(func=func),
#                 )
#
#             return to_rx(result, subscribe_schduler=info.multicast_scheduler)
#
#     return MultiCast(FromEventMultiCast())


def merge(
        *sources: MultiCast
):
    """ Merges zero or more `MultiCast` streams together
    """

    if len(sources) == 0:
        return empty()

    elif len(sources) == 1:
        return sources[0]

    else:
        return sources[0].pipe(
            merge_op(*sources[1:])
        )


def collect_flowables(
      *sources: MultiCast,
):
    if len(sources) == 0:
        return empty()

    elif len(sources) == 1:
        return sources

    else:
        return sources[0].pipe(
            rxbp.multicast.op.collect_flowables(*sources[1:])
        )
