from typing import Iterable

from rxbp.flowable.init import init_flowable
from rxbp.flowabletree.nodes import FlowableNode
from rxbp.flowabletree.sources.fromiterable import init_from_iterable
from rxbp.flowabletree.sources.fromvalue import init_from_value
from rxbp.flowabletree.operations.merge.flowable import init_merge
from rxbp.flowabletree.operations.zip.flowable import init_zip


def from_iterable(iterable: Iterable):
    return init_flowable(
        child=init_from_iterable(iterable=iterable)
    )


def from_value(value):
    return init_flowable(
        child=init_from_value(value=value),
    )


def merge(observables: tuple[FlowableNode, ...]):
    return init_flowable(
        child=init_merge(children=observables),
    )


def zip(observables: tuple[FlowableNode, ...]):
    return init_flowable(
        child=init_zip(children=observables),
    )
