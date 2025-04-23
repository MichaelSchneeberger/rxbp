from typing import Iterable

from rxbp.flowable.init import init_flowable
from rxbp.flowabletree.sources.fromrx import FromRx
from rxbp.flowabletree.nodes import FlowableNode
from rxbp.flowabletree.operations.buffer.flowable import init_buffer
from rxbp.flowabletree.sources.connectable import init_connectable
from rxbp.flowabletree.sources.fromiterable import init_from_iterable
from rxbp.flowabletree.sources.fromvalue import init_from_value
from rxbp.flowabletree.operations.merge.flowable import init_merge
from rxbp.flowabletree.operations.zip.flowable import init_zip


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


def zip(observables: tuple[FlowableNode, ...]):
    return init_flowable(
        child=init_zip(children=observables),
    )
