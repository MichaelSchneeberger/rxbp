from __future__ import annotations

from dataclasses import replace
from typing import override
from dataclassabc import dataclassabc

from rxbp.flowable.flowable import Flowable
from rxbp.flowabletree.nodes import FlowableNode


@dataclassabc(frozen=True)
class FlowableImpl[V](Flowable[V]):
    child: FlowableNode[V]

    @override
    def copy(self, /, **changes) -> FlowableImpl[V]:
        return replace(self, **changes)


def init_flowable[V](child: FlowableNode[V]):
    return FlowableImpl[V](child=child)
