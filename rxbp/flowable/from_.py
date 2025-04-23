from typing import Iterable

from continuationmonad.typing import Scheduler

from rxbp.flowable.init import init_flowable
from rxbp.flowabletree.sources.fromrx import FromRx
from rxbp.flowabletree.nodes import FlowableNode
from rxbp.flowabletree.operations.buffer.flowable import init_buffer
from rxbp.flowabletree.sources.connectable import init_connectable
from rxbp.flowabletree.sources.fromiterable import init_from_iterable
from rxbp.flowabletree.sources.fromvalue import init_from_value
from rxbp.flowabletree.operations.merge.flowable import init_merge
from rxbp.flowabletree.operations.zip.flowable import init_zip
from rxbp.flowabletree.sources.scheduleon import init_schedule_on


def connectable(id, init):
    return init_flowable(child=init_connectable(id, init))


def from_iterable(iterable: Iterable):
    return init_flowable(
        child=init_from_iterable(iterable=iterable)
    )


def from_value(value):
    return init_flowable(
        child=init_from_value(value=value),
    )


def from_rx(source):
    return init_flowable(
        child=init_buffer(
            child=FromRx(source=source),
        ),
    )


def merge(observables: tuple[FlowableNode, ...]):
    return init_flowable(
        child=init_merge(children=observables),
    )


def schedule_on(scheduler: Scheduler):
    return init_flowable(init_schedule_on(scheduler=scheduler))


def zip(observables: tuple[FlowableNode, ...]):
    return init_flowable(
        child=init_zip(children=observables),
    )
