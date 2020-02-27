from typing import Any, Callable, Union, Dict

import rx
import rxbp
from rxbp.flowable import Flowable
from rxbp.flowablebase import FlowableBase
from rxbp.flowables.refcountflowable import RefCountFlowable
from rxbp.flowables.subscribeonflowable import SubscribeOnFlowable
from rxbp.multicast.flowablestatemixin import FlowableStateMixin
from rxbp.multicast.multicast import MultiCast
from rxbp.multicast.multicastInfo import MultiCastInfo
from rxbp.multicast.multicastbase import MultiCastBase
from rxbp.multicast.multicastflowable import MultiCastFlowable
from rxbp.multicast.op import merge as merge_op
from rxbp.subscriber import Subscriber
from rxbp.subscription import Subscription
from rxbp.torx import to_rx


def empty():

    class FromObjectMultiCast(MultiCastBase):
        def get_source(self, info: MultiCastInfo) -> rx.typing.Observable:
            return rx.empty(scheduler=info.multicast_scheduler)

    return MultiCast(FromObjectMultiCast())


# def from_flowable(
#         *source: Union[Flowable, Dict[Any, Flowable], FlowableStateMixin],
# ):
#     def return_value(val):
#         class FromObjectMultiCast(MultiCastBase):
#             def get_source(self, info: MultiCastInfo) -> rx.typing.Observable:
#                 # first element has to be scheduled on dedicated scheduler. Unlike in rxbp,
#                 # this "subscribe scheduler" is not automatically provided in rx, that is
#                 # why it must be provided as the `scheduler` argument of the `return_value`
#                 # operator.
#                 return rx.return_value(val, scheduler=info.multicast_scheduler)
#
#         return MultiCast(FromObjectMultiCast())
#
#     if len(source) == 0:
#         return empty()
#
#     def share_flowable(f: Flowable):
#         # class MultiCastFlowable(FlowableBase):
#         #     def __init__(self, source: FlowableBase):
#         #         self.source = source
#         #
#         #     def unsafe_subscribe(self, subscriber: Subscriber) -> Subscription:
#         #         new_subscriber = Subscriber(
#         #             scheduler=subscriber.scheduler,
#         #             subscribe_scheduler=subscriber.subscribe_scheduler,
#         #             is_multicasted=True,
#         #         )
#         #
#         #         return self.source.unsafe_subscribe(new_subscriber)
#         # return Flowable(MultiCastFlowable(source=f)).share()
#         return f.share()
#
#     first = source[0]
#     if isinstance(first, Flowable):
#         assert all(isinstance(s, Flowable) for s in source)
#
#         if len(source) == 1:
#             val = share_flowable(source[0])
#         else:
#             val = [share_flowable(s).share() for s in source]
#         multicast = return_value(val)
#
#     elif isinstance(first, list):
#         val = [share_flowable(s).share() for s in first]
#         multicast = return_value(val)
#
#     elif isinstance(first, dict):
#
#         val = {key: share_flowable(s).share() for key, s in first.items()}
#         multicast = return_value(val)
#
#     elif isinstance(first, FlowableStateMixin):
#         state = first.get_flowable_state()
#
#         val = {key: share_flowable(s).share() for key, s in state.items()}
#         multicast = return_value(val)
#
#     else:
#         raise Exception(f'unexpected argument "{first}"')
#
#     return multicast


def from_flowable(
        *source: Union[Flowable, Dict[Any, Flowable], FlowableStateMixin],
):
    # def return_value(is_list: bool = None, is_dict: bool = None):
    #     if is_list is True:
    #         init_val = []
    #     elif is_dict is True:
    #         init_val = {}
    #     else:
    #         init_val = {}
    #
    #     class FromObjectMultiCast(MultiCastBase):
    #         def get_source(self, info: MultiCastInfo) -> rx.typing.Observable:
    #             # first element has to be scheduled on dedicated scheduler. Unlike in rxbp,
    #             # this "subscribe scheduler" is not automatically provided in rx, that is
    #             # why it must be provided as the `scheduler` argument of the `return_value`
    #             # operator.
    #             return rx.return_value(init_val, scheduler=info.multicast_scheduler)
    #
    #     return MultiCast(FromObjectMultiCast())

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


def connect_flowable(
      *sources: MultiCast,
):
    if len(sources) == 0:
        return empty()

    elif len(sources) == 1:
        return sources

    else:
        return sources[0].pipe(
            rxbp.multicast.op.connect_flowable(*sources[1:])
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
