from typing import Any, Callable, Iterable

import rx
from rx.disposable import CompositeDisposable

import rxbp
from rxbp.acknowledgement.ack import Ack
from rxbp.acknowledgement.continueack import continue_ack
from rxbp.flowable import Flowable
from rxbp.init.initobserverinfo import init_observer_info
from rxbp.init.initsubscriber import init_subscriber
from rxbp.multicast.imperative.imperativemulticastbuild import ImperativeMultiCastBuild
from rxbp.multicast.imperative.imperativemulticastbuilder import ImperativeMultiCastBuilder
from rxbp.multicast.init.initmulticast import init_multicast
from rxbp.multicast.init.initmulticastsubscription import init_multicast_subscription
from rxbp.multicast.mixins.multicastmixin import MultiCastMixin
from rxbp.multicast.multicast import MultiCast
from rxbp.multicast.multicastobservable import MultiCastObservable
from rxbp.multicast.multicastobservables.fromiterableobservable import FromIterableObservable
from rxbp.multicast.multicastobserverinfo import MultiCastObserverInfo
from rxbp.multicast.multicasts.emptymulticast import EmptyMultiCast
from rxbp.multicast.multicastsubscriber import MultiCastSubscriber
from rxbp.multicast.multicastsubscription import MultiCastSubscription
from rxbp.multicast.op import merge as merge_op
from rxbp.multicast.typing import MultiCastItem
from rxbp.observer import Observer


def empty():
    """
    create a MultiCast emitting no elements
    """

    return init_multicast(EmptyMultiCast())


def build_imperative_multicast(
        func: Callable[[ImperativeMultiCastBuilder], ImperativeMultiCastBuild],
        composite_disposable: CompositeDisposable = None,
):
    class BuildBlockingFlowableMultiCast(MultiCastMixin):
        def unsafe_subscribe(self, subscriber: MultiCastSubscriber) -> MultiCastSubscription:

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
            ).materialize()

            return imperative_call.output_selector(
                flowable,
            ).unsafe_subscribe(subscriber=subscriber)

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
                observable=FromIterableObservable(
                    values=[value],
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


# def from_rx_observable(val: rx.typing.Observable):
#     """
#     Create a *MultiCast* from an *rx.Observable*.
#     """
#
#     class FromObservableMultiCast(MultiCastMixin):
#         def unsafe_subscribe(self, subscriber: MultiCastSubscriber) -> MultiCastSubscription:
#             return val
#
#     return init_multicast(FromObservableMultiCast())


def from_flowable(
        source: Flowable,
):
    """
    Create a MultiCast that emit each element received by the Flowable.
    """

    class FromFlowableMultiCast(MultiCastMixin):
        def unsafe_subscribe(self, subscriber: MultiCastSubscriber) -> Flowable:
            subscription = source.unsafe_subscribe(init_subscriber(
                scheduler=subscriber.source_scheduler,
                subscribe_scheduler=subscriber.source_scheduler,
            ))

            class FromFlowableMultiCastObservable(MultiCastObservable):
                def observe(self, observer_info: MultiCastObserverInfo) -> rx.typing.Disposable:
                    class FromFlowableMultiCastObserver(Observer):
                        def on_next(self, elem: MultiCastItem) -> Ack:
                            observer_info.observer.on_next(elem)
                            return continue_ack

                        def on_error(self, exc: Exception) -> None:
                            observer_info.observer.on_error(exc)

                        def on_completed(self) -> None:
                            observer_info.observer.on_completed()

                    subscription.observable.observe(init_observer_info(
                        observer=FromFlowableMultiCastObserver()
                    ))

            return init_multicast_subscription(
                observable=FromFlowableMultiCastObservable(),
            )

    return init_multicast(FromFlowableMultiCast())


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
