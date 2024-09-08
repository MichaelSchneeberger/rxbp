from typing import Iterable
from rxbp.flowable.init import init_flowable
from rxbp.flowabletree.init import init_from_iterable, init_from_value, init_zip
from rxbp.flowabletree.nodes import FlowableNode


def from_iterable[V](iterable: Iterable[V]):
    return init_flowable(
        child=init_from_iterable(iterable=iterable)
    )


def from_value[V](value: V):
    return init_flowable(
        child=init_from_value(value=value),
    )


def zip(observables: tuple[FlowableNode, ...]):
    return init_flowable(
        child=init_zip(children=observables),
    )
