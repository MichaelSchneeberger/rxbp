from __future__ import annotations

from typing import override
from dataclasses import dataclass

from rxbp.flowable.flowable import ConnectableFlowable, Flowable, SeqFlowable
from rxbp.flowabletree.flowablenode import FlowableNode


@dataclass(frozen=True)
class FlowableImpl[U](Flowable[U]):
    child: FlowableNode[S, U]

    @override
    def copy[V](self, /, child: FlowableNode[V]) -> FlowableImpl[V]:
        return init_flowable(child=child)
    
    @override
    def seq(self):
        return init_seq_flowable(child=self.child)


def init_flowable[U](child: FlowableNode[S, U]):
    return FlowableImpl[U](child=child)



@dataclass(frozen=True)
class SeqFlowableImpl[U](SeqFlowable[U]):
    child: FlowableNode[S, U]

    @override
    def copy[V](self, /, child: FlowableNode[V]) -> FlowableImpl[V]:
        return init_flowable(child=child)


def init_seq_flowable[U](child: FlowableNode[S, U]):
    return SeqFlowableImpl[U](child=child)



@dataclass(frozen=True)
class ConnectableFlowableImpl[U](FlowableImpl[U], ConnectableFlowable[U]):
    pass
    # child: FlowableNode[S, U]

    # @override
    # def copy[V](self, /, child: FlowableNode[V]) -> FlowableImpl[V]:
    #     return init_flowable(child=child)


def init_connectable_flowable[U](child: FlowableNode[S, U]):
    return ConnectableFlowableImpl[U](child=child)
