from typing import Any, Callable, Iterable

import rx
from rx.disposable import CompositeDisposable

import rxbp
from rxbp.flowable import Flowable
from rxbp.flowables.subscribeonflowable import SubscribeOnFlowable
from rxbp.multicast.imperative.imperativemulticastbuild import ImperativeMultiCastBuild
from rxbp.multicast.imperative.imperativemulticastbuilder import ImperativeMultiCastBuilder
from rxbp.multicast.init.initmulticast import init_multicast
from rxbp.multicast.init.initmulticastsubscription import init_multicast_subscription
from rxbp.multicast.mixins.multicastmixin import MultiCastMixin
from rxbp.multicast.multicast import MultiCast
from rxbp.multicast.multicastobservables.fromiterableobservable import FromIterableObservable
from rxbp.multicast.multicastobservables.returnvaluemulticastobservable import ReturnValueMultiCastObservable
from rxbp.multicast.multicastsubscriber import MultiCastSubscriber
from rxbp.multicast.multicastsubscription import MultiCastSubscription
from rxbp.multicast.op import merge as merge_op
from rxbp.multicast.typing import MultiCastItem
from rxbp.torx import to_rx


def empty():
    """
    create a MultiCast emitting no elements
    """

    class EmptyMultiCast(MultiCastMixin):
        def unsafe_subscribe(self, subscriber: MultiCastSubscriber) -> rx.typing.Observable:
            return rx.empty(scheduler=subscriber.multicast_scheduler)

    return init_multicast(EmptyMultiCast())


def build_imperative_multicast(
    func: Callable[[ImperativeMultiCastBuilder], ImperativeMultiCastBuild],
    composite_disposable: CompositeDisposable = None,
):

    class BuildBlockingFlowableMultiCast(MultiCastMixin):
        def unsafe_subscribe(self, subscriber: MultiCastSubscriber) -> rx.typing.Observable[MultiCastItem]:

            def on_completed():
                for subject in imperative_call.subjects:
                    subject.on_completed()

            def on_error(exc: Exception):
                for subject in imperative_call.subjects:
                    subject.on_error(exc)

            composite_disposable_ = composite_disposable or CompositeDisposable()

            builder = ImperativeMultiCastBuilder(
                composite_disposable=composite_disposable_,
                scheduler=subscriber.source_scheduler,
            )

            imperative_call = func(builder)

            flowable = imperative_call.blocking_flowable.pipe(
                rxbp.op.do_action(
                    on_disposed=lambda: composite_disposable_.dispose(),
                    on_completed=on_completed,
                    on_error=on_error,
                ),
            )

            return imperative_call.output_selector(
                flowable,
            ).get_source(info=info)

    return init_multicast(BuildBlockingFlowableMultiCast())


# def return_flowable(
#         val: Union[
#             Flowable,
#             List[Flowable],
#             Dict[Any, Flowable],
#             FlowableStateMixin,
#         ],
# ):
#     """
#     Turn zero or more Flowables into multi-cast Flowables emitted as a single
#     element inside a MultiCast.
#
#     :param val: an object that may contain a non-multi-casted Flowables
#     """
#
#     def to_multi_cast_flowable(inner_val: Flowable):
#         if isinstance(inner_val, Flowable):
#             # assert isinstance(f, Flowable), f'{f} is not a Flowable'
#             assert not isinstance(inner_val, MultiCastFlowable), \
#                 "it's not allowed to multicast a multi-casted Flowable"
#
#             return init_multicast_flowable(RefCountFlowable(inner_val))
#         else:
#             return inner_val
#
#     if isinstance(val, Flowable):
#         return_val = to_multi_cast_flowable(val)
#
#     elif isinstance(val, list):
#         return_val = [to_multi_cast_flowable(s) for s in val]
#
#     elif isinstance(val, dict):
#         return_val = {key: to_multi_cast_flowable(s) for key, s in val.items()}
#
#     elif isinstance(val, FlowableStateMixin):
#         state = val.get_flowable_state()
#         new_state = {key: to_multi_cast_flowable(s) for key, s in state.items()}
#         return_val = val.set_flowable_state(new_state)
#
#     else:
#         raise Exception(f'unexpected argument "{val}"')
#
#     return return_value(return_val)


def return_value(value: Any):
    """
    Create a MultiCast that emits a single element.

    :param val: The single element emitted by the MultiCast
    """

    class ReturnValueMultiCast(MultiCastMixin):
        def unsafe_subscribe(self, subscriber: MultiCastSubscriber) -> MultiCastSubscription:

            # first element has to be scheduled on dedicated scheduler. Unlike in rxbp,
            # this "subscribe scheduler" is not automatically provided in rx, that is
            # why it must be provided as the `scheduler` argument of the `return_flowable`
            # operator.
            return init_multicast_subscription(
                observable=ReturnValueMultiCastObservable(
                    value=value,
                    subscribe_scheduler=subscriber.multicast_scheduler,
                ),
            )

    return init_multicast(ReturnValueMultiCast())


def from_iterable(values: Iterable[Any]):
    """
    Create a *MultiCast* emitting elements taken from an iterable.

    :param vals: the iterable whose elements are sent
    """

    class FromIterableMultiCast(MultiCastMixin):
        def unsafe_subscribe(self, subscriber: MultiCastSubscriber) -> MultiCastSubscription:
            return init_multicast_subscription(FromIterableObservable(
                values=values,
                subscribe_scheduler=subscriber.multicast_scheduler,
            ))

    return init_multicast(FromIterableMultiCast())


def from_rx_observable(val: rx.typing.Observable):
    """
    Create a *MultiCast* from an *rx.Observable*.
    """

    class FromObservableMultiCast(MultiCastMixin):
        def unsafe_subscribe(self, subscriber: MultiCastSubscriber) -> Flowable:
            return val

    return init_multicast(FromObservableMultiCast())


# def from_flowable(
#         source: Flowable,
# ):
#     """
#     Create a MultiCast that emit each element received by the Flowable.
#     """
#
#     class FromEventMultiCast(MultiCastMixin):
#         def unsafe_subscribe(self, subscriber: MultiCastSubscriber) -> Flowable:
#             result = Flowable(SubscribeOnFlowable(source, scheduler=subscriber.multicast_scheduler))
#
#             return to_rx(result, subscribe_schduler=subscriber.multicast_scheduler)
#
#     return init_multicast(FromEventMultiCast())


def merge(
        *sources: MultiCast
):
    """
    Merge the elements of the *MultiCast* sequences into a single *MultiCast*.
    """

    if len(sources) == 0:
        return empty()

    elif len(sources) == 1:
        return sources[0]

    else:
        return sources[0].pipe(
            merge_op(*sources[1:])
        )


def join_flowables(
      *sources: MultiCast,
):
    """
    Zips MultiCasts emitting a single Flowable to a MultiCast emitting a single tuple
    of Flowables.
    """

    if len(sources) == 0:
        return empty()

    elif len(sources) == 1:
        return sources[0]

    else:
        return sources[0].pipe(
            rxbp.multicast.op.join_flowables(*sources[1:])
        )
