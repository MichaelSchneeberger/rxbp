from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TypeVar, override

from rxbp.flowabletree.assignvirtualcount import AssignVirtualCount
from rxbp.flowabletree.observer import Observer
from rxbp.flowabletree.subscriptionresult import SubscriptionResult



E = TypeVar("E", contravariant=True)

class FlowableNode[S, A, U, E](AssignVirtualCount[S], ABC):
    @abstractmethod
    def unsafe_subscribe(
        self,
        state: S,
        observer: Observer[U, E],
        args: A, #SubscribeArgs,
    ) -> tuple[S, SubscriptionResult]:
        """
        state: object that is passed through the entire tree structure
        """

    @override
    def discover(self, state: S) -> S:
        return state

    @override
    def assign_virtual_count(self, state: S, virtual_count: int) -> S:
        return state


class SingleChildFlowableNode[S, A, U, V, E](FlowableNode[S, A, V, E], ABC):
    """
    Represents a state monad node with a single child.
    """

    @property
    @abstractmethod
    def p_child(self) -> FlowableNode[S, A, U, E]: ...

    @override
    def discover(self, state: S) -> S:
        return self.p_child.discover(state)

    @override
    def assign_virtual_count(self, state: S, virtual_count: int) -> S:
        return self.p_child.assign_virtual_count(state, virtual_count)


class TwoChildrenFlowableNode[S, A, L, R, V, E](FlowableNode[S, A, V, E], ABC):
    """
    Represents a state monad node with two children.
    """

    @property
    @abstractmethod
    def p_left(self) -> FlowableNode[S, A, L, E]: ...

    @property
    @abstractmethod
    def p_right(self) -> FlowableNode[S, A, R, E]: ...

    @override
    def discover(
        self,
        state: S,
    ) -> S:
        state = self.p_left.discover(state)
        return self.p_right.discover(state)

    @override
    def assign_virtual_count(
        self,
        state: S,
        virtual_count: int,
    ) -> S:
        state = self.p_left.assign_virtual_count(state, virtual_count)
        return self.p_right.assign_virtual_count(state, virtual_count)


class MultiChildrenFlowableNode[S, A, U, V, E](FlowableNode[S, A, V, E], ABC):
    """
    Represents a state monad node with two children.
    """

    @property
    @abstractmethod
    def p_children(self) -> tuple[FlowableNode[S, A, U, E], ...]: ...

    @override
    def discover(
        self,
        state: S,
    ) -> S:
        for child in self.p_children:
            state = child.discover(state)
        return state

    @override
    def assign_virtual_count(
        self,
        state: S,
        virtual_count: int,
    ) -> S:
        for child in self.p_children:
            state = child.assign_virtual_count(state, virtual_count)
        return state

