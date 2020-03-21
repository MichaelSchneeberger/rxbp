from typing import Any, Callable, Union, Dict, Iterable, List

import rx
from rx.disposable import CompositeDisposable

import rxbp
from rxbp.flowable import Flowable
from rxbp.flowables.refcountflowable import RefCountFlowable
from rxbp.flowables.subscribeonflowable import SubscribeOnFlowable
from rxbp.multicast.flowablestatemixin import FlowableStateMixin
from rxbp.multicast.imperative.imperativemulticastbuild import ImperativeMultiCastBuild
from rxbp.multicast.imperative.imperativemulticastbuilder import ImperativeMultiCastBuilder
from rxbp.multicast.multicast import MultiCast
from rxbp.multicast.multicastInfo import MultiCastInfo
from rxbp.multicast.multicastbase import MultiCastBase
from rxbp.multicast.multicastflowable import MultiCastFlowable
from rxbp.multicast.op import merge as merge_op
from rxbp.multicast.typing import MultiCastValue
from rxbp.torx import to_rx


def empty():
    """
    create a MultiCast emitting no elements
    """

    class EmptyMultiCast(MultiCastBase):
        def get_source(self, info: MultiCastInfo) -> rx.typing.Observable:
            return rx.empty(scheduler=info.multicast_scheduler)

    return MultiCast(EmptyMultiCast())


def build_imperative_multicast(
    func: Callable[[ImperativeMultiCastBuilder], ImperativeMultiCastBuild],
    composite_disposable: CompositeDisposable = None,
):

    class BuildBlockingFlowableMultiCast(MultiCastBase):
        def get_source(self, info: MultiCastInfo) -> rx.typing.Observable[MultiCastValue]:

            def on_completed():
                for subject in imperative_call.subjects:
                    subject.on_completed()

            def on_error(exc: Exception):
                for subject in imperative_call.subjects:
                    subject.on_error(exc)

            composite_disposable_ = composite_disposable or CompositeDisposable()

            builder = ImperativeMultiCastBuilder(
                composite_disposable=composite_disposable_,
                scheduler=info.source_scheduler,
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

    return MultiCast(BuildBlockingFlowableMultiCast())


def return_flowable(
        val: Union[
            Flowable,
            List[Flowable],
            Dict[Any, Flowable],
            FlowableStateMixin,
        ],
):
    """
    Turn zero or more Flowables into multi-cast Flowables emitted as a single
    element inside a MultiCast.

    :param val: an object that may contain a non-multi-casted Flowables
    """

    def to_multi_cast_flowable(inner_val: Flowable):
        if isinstance(inner_val, Flowable):
            # assert isinstance(f, Flowable), f'{f} is not a Flowable'
            assert not isinstance(inner_val, MultiCastFlowable), \
                "it's not allowed to multicast a multi-casted Flowable"

            return MultiCastFlowable(RefCountFlowable(inner_val))
        else:
            return inner_val

    if isinstance(val, Flowable):
        return_val = to_multi_cast_flowable(val)

    elif isinstance(val, list):
        return_val = [to_multi_cast_flowable(s) for s in val]

    elif isinstance(val, dict):
        return_val = {key: to_multi_cast_flowable(s) for key, s in val.items()}

    elif isinstance(val, FlowableStateMixin):
        state = val.get_flowable_state()
        new_state = {key: to_multi_cast_flowable(s) for key, s in state.items()}
        return_val = val.set_flowable_state(new_state)

    else:
        raise Exception(f'unexpected argument "{val}"')

    return return_value(return_val)


def return_value(val: Any):
    """
    Create a MultiCast that emits a single element.

    :param val: The single element emitted by the MultiCast
    """

    class ReturnValueMultiCast(MultiCastBase):
        def get_source(self, info: MultiCastInfo) -> rx.typing.Observable:
            # first element has to be scheduled on dedicated scheduler. Unlike in rxbp,
            # this "subscribe scheduler" is not automatically provided in rx, that is
            # why it must be provided as the `scheduler` argument of the `return_flowable`
            # operator.
            return rx.return_value(val, scheduler=info.multicast_scheduler)

    return MultiCast(ReturnValueMultiCast())


def from_iterable(vals: Iterable[Any]):
    """
    Create a *MultiCast* emitting elements taken from an iterable.

    :param vals: the iterable whose elements are sent
    """

    class FromIterableMultiCast(MultiCastBase):
        def get_source(self, info: MultiCastInfo) -> Flowable:
            return rx.from_(vals, scheduler=info.multicast_scheduler)

    return MultiCast(FromIterableMultiCast())


def from_rx_observable(val: rx.typing.Observable):
    """
    Create a *MultiCast* from an *rx.Observable*.
    """

    class FromObservableMultiCast(MultiCastBase):
        def get_source(self, info: MultiCastInfo) -> Flowable:
            return val

    return MultiCast(FromObservableMultiCast())


def from_flowable(
        source: Flowable,
):
    """
    Create a MultiCast that emit each element received by the Flowable.
    """

    class FromEventMultiCast(MultiCastBase):
        def get_source(self, info: MultiCastInfo) -> Flowable:
            result = Flowable(SubscribeOnFlowable(source, scheduler=info.multicast_scheduler))

            return to_rx(result, subscribe_schduler=info.multicast_scheduler)

    return MultiCast(FromEventMultiCast())


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
        return sources

    else:
        return sources[0].pipe(
            rxbp.multicast.op.join_flowables(*sources[1:])
        )
