from typing import Any, Callable, Iterable

from rx.disposable import CompositeDisposable

import rxbp
from rxbp.flowable import Flowable
from rxbp.multicast.imperative.imperativemulticastbuild import ImperativeMultiCastBuild
from rxbp.multicast.imperative.imperativemulticastbuilder import ImperativeMultiCastBuilder
from rxbp.multicast.init.initmulticast import init_multicast
from rxbp.multicast.mixins.multicastmixin import MultiCastMixin
from rxbp.multicast.multicast import MultiCast
from rxbp.multicast.multicasts.emptymulticast import EmptyMultiCast
from rxbp.multicast.multicasts.fromflowablemulticast import FromFlowableMultiCast
from rxbp.multicast.multicasts.fromiterablemulticast import FromIterableMultiCast
from rxbp.multicast.multicastsubscriber import MultiCastSubscriber
from rxbp.multicast.multicastsubscription import MultiCastSubscription
from rxbp.multicast.op import merge as merge_op


def empty():
    """
    create a MultiCast emitting no elements
    """

    return init_multicast(EmptyMultiCast(
        scheduler_index=1,
    ))


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
                subscriber=subscriber,
                # source_scheduler=subscriber.source_scheduler,
                # multicast_scheduler=subscriber.multicast_scheduler,
            )

            imperative_call = func(builder)

            flowable = imperative_call.blocking_flowable.pipe(
                rxbp.op.do_action(
                    on_disposed=lambda: composite_disposable_.dispose(),
                    on_completed=on_completed,
                    on_error=on_error,
                ),
            ).materialize().share()

            return imperative_call.output_selector(
                flowable,
            ).unsafe_subscribe(subscriber=subscriber)

    return init_multicast(BuildBlockingFlowableMultiCast())


def return_value(value: Any):
    """
    Create a MultiCast that emits a single element.

    :param val: The single element emitted by the MultiCast
    """

    return init_multicast(FromIterableMultiCast(
        values=[value],
        scheduler_index=1,
    ))


def from_iterable(values: Iterable[Any]):
    """
    Create a *MultiCast* emitting elements taken from an iterable.

    :param vals: the iterable whose elements are sent
    """

    return init_multicast(FromIterableMultiCast(
        values=values,
        scheduler_index=1,
    ))


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

    return init_multicast(FromFlowableMultiCast(source=source))


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

    else:
        return sources[0].pipe(
            rxbp.multicast.op.join_flowables(*sources[1:])
        )
