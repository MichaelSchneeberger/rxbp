from typing import Iterable
from dataclassabc import dataclassabc

from rxbp.flowabletree.nodes import FlowableNode
from rxbp.flowabletree.operations.fromiterable import FromIterable
from rxbp.flowabletree.operations.fromvalue import FromValue
from rxbp.flowabletree.operations.shared import Shared
from rxbp.flowabletree.operations.zip_ import Zip


@dataclassabc(frozen=True)
class FromIterableImpl[V](FromIterable[V]):
    iterable: Iterable[V]


def init_from_iterable[V](iterable: Iterable[V]):
    return FromIterableImpl[V](iterable=iterable)


@dataclassabc(frozen=True)
class FromValueImpl[V](FromValue[V]):
    value: V


def init_from_value[V](value: V):
    return FromValueImpl[V](value=value)


@dataclassabc(frozen=True)
class SharedImpl[V](Shared[V]):
    child: FlowableNode[V]


def init_shared[V](child: FlowableNode[V]):
    return SharedImpl(child=child)


@dataclassabc(frozen=True)
class ZipImpl[V](Zip[V]):
    children: tuple[FlowableNode, ...]


def init_zip[V](children: tuple[FlowableNode[V], ...]):
    return ZipImpl[V](children=children)
