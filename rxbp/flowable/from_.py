from typing import Never

from contcert.typing import ContCert
from rxbp.flowable.flowable import Flowable, init_flowable
from rxbp.flowabletree.flowablenode import FlowableNode
from rxbp.flowabletree.operations.merge.flowable import init_merge_flowable_node
from rxbp.flowabletree.sources.create.create import init_create
from rxbp.flowabletree.sources.create.operations import CreateSubscribeArgs
from rxbp.flowabletree.state import State
from rxsingle.abc import (
    RxSingleNode,
)


# def connectable(id, init):
#     return init_connectable_flowable(child=init_connectable(id, init))
#
#
# def count():
#     return init_flowable(_count())


def create[S: State, U, E](
    source: RxSingleNode[S, CreateSubscribeArgs[U, E], ContCert, Never],
) -> Flowable[S, U, E]:
    return init_flowable(init_create(source=source))


# def empty():
#     return init_flowable(_empty())
#
#
# def error(exception: Exception):
#     return init_flowable(_error(exception))
#
#
# def from_iterable[U](iterable: Iterable[U]):
#     return init_flowable(_from_iterable(iterable))
#
#
# def from_value(value):
#     return init_flowable(_from_value(value))
#
#
# def from_rx(source):
#     return init_flowable(
#         child=init_buffer(
#             child=FromRx(source=source),
#         ),
#     )
#
#
# def interval(seconds: float, scheduler: Scheduler | None = None):
#     return init_flowable(_interval(seconds, scheduler))


def merge[S: State, U, E](observables: tuple[FlowableNode[S, U, E], ...]):
    return init_flowable(
        child=init_merge_flowable_node(children=observables),
    )


# def repeat(value):
#     return init_flowable(
#         child=_repeat(value=value),
#     )
#
#
# def schedule_on(scheduler: Scheduler | None = None):
#     return init_flowable(_schedule_on(scheduler))
#
#
# def schedule_relative(seconds: float, scheduler: Scheduler | None = None):
#     return init_flowable(_schedule_relative(seconds, scheduler))
#
#
# def schedule_absolute(until: datetime.datetime, scheduler: Scheduler | None = None):
#     return init_flowable(_schedule_absolute(until, scheduler))
#
#
# def zip(observables: tuple[FlowableNode, ...]):
#     return init_flowable(
#         child=init_zip_flowable_node(children=observables),
#     )
