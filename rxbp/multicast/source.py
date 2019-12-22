from typing import Any, Callable, Union, Dict

import rx
import rxbp
from rxbp.flowable import Flowable
from rxbp.flowables.subscribeonflowable import SubscribeOnFlowable
from rxbp.multicast.flowablestatemixin import FlowableStateMixin
from rxbp.multicast.multicast import MultiCast
from rxbp.multicast.multicastInfo import MultiCastInfo
from rxbp.multicast.multicastbase import MultiCastBase
from rxbp.multicast.op import merge as merge_op
from rxbp.torx import to_rx


def empty():

    class FromObjectMultiCast(MultiCastBase):
        def get_source(self, info: MultiCastInfo) -> rx.typing.Observable:
            return rx.empty(scheduler=info.multicast_scheduler)

    return MultiCast(FromObjectMultiCast())


def from_flowable(
        *source: Union[Flowable, Dict[Any, Flowable], FlowableStateMixin],
):
    def return_value(is_list: bool = None, is_dict: bool = None):
        if is_list is True:
            init_val = []
        elif is_dict is True:
            init_val = {}
        else:
            init_val = {}

        class FromObjectMultiCast(MultiCastBase):
            def get_source(self, info: MultiCastInfo) -> rx.typing.Observable:
                # first element has to be scheduled on dedicated scheduler. Unlike in rxbp,
                # this "subscribe scheduler" is not automatically provided in rx, that is
                # why it must be provided as the `scheduler` argument of the `return_value`
                # operator.
                return rx.return_value(init_val, scheduler=info.multicast_scheduler)

        return MultiCast(FromObjectMultiCast())

    if len(source) == 0:
        return empty()

    first = source[0]

    if isinstance(first, Flowable):
        assert all(isinstance(s, Flowable) for s in source)

        multicast = return_value(is_list=True)

        for s in source:
            def for_func(s=s):
                return multicast.pipe(
                    rxbp.multicast.op.extend(lambda _: s),
                )

            multicast = for_func()

    elif isinstance(first, list):
        multicast = return_value(is_list=True)

        for val in first:
            def for_func(val=val):
                return multicast.pipe(
                    rxbp.multicast.op.extend(lambda _: [val]),
                )

            multicast = for_func()

    elif isinstance(first, dict):

        multicast = return_value(is_dict=True)

        for key, s in first.items():
            def for_func(key=key, s=s):
                return multicast.pipe(
                    rxbp.multicast.op.extend(lambda _: {key: s}),
                )

            multicast = for_func()

    elif isinstance(first, FlowableStateMixin):

        multicast = return_value(is_dict=True)

        state = first.get_flowable_state()

        for key, s in state.items():
            def for_func(key=key, s=s):
                return multicast.pipe(
                    rxbp.multicast.op.extend(lambda _: {key: s}),
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

    class FromEventMultiCast(MultiCastBase):
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

    return MultiCast(FromEventMultiCast())


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


def zip(
      *sources: MultiCast,
):
    if len(sources) == 0:
        return empty()

    elif len(sources) == 1:
        return sources

    else:
        return sources[0].pipe(
            rxbp.multicast.op.zip(*sources[1:])
        )

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
