from __future__ import annotations

from typing import override
from dataclassabc import dataclassabc

from rxbp.flowable.flowable import ConnectableFlowable, Flowable, SeqFlowable
from rxbp.flowabletree.nodes import FlowableNode


@dataclassabc(frozen=True)
class FlowableImpl[U](Flowable[U]):
    child: FlowableNode[U]

    @override
    def copy[V](self, /, child: FlowableNode[V]) -> FlowableImpl[V]:
        return init_flowable(child=child)
    
    @override
    def seq(self):
        return init_seq_flowable(child=self.child)


def init_flowable[U](child: FlowableNode[U]):
    return FlowableImpl[U](child=child)



@dataclassabc(frozen=True)
class SeqFlowableImpl[U](SeqFlowable[U]):
    child: FlowableNode[U]

    @override
    def copy[V](self, /, child: FlowableNode[V]) -> FlowableImpl[V]:
        return init_flowable(child=child)


def init_seq_flowable[U](child: FlowableNode[U]):
    return SeqFlowableImpl[U](child=child)



@dataclassabc(frozen=True)
class ConnectableFlowableImpl[U](ConnectableFlowable[U]):
    child: FlowableNode[U]

    @override
    def copy[V](self, /, child: FlowableNode[V]) -> FlowableImpl[V]:
        return init_flowable(child=child)


def init_connectable_flowable[U](child: FlowableNode[U]):
    return ConnectableFlowableImpl[U](child=child)
