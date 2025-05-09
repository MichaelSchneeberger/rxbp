from __future__ import annotations

from abc import abstractmethod

from rxbp.state import State
from rxbp.flowabletree.assignweightmixin import AssignWeightMixin
from rxbp.flowabletree.subscribeargs import SubscribeArgs
from rxbp.flowabletree.subscriptionresult import SubscriptionResult


class FlowableNode[U](AssignWeightMixin):
    @abstractmethod
    def unsafe_subscribe(
        self,
        state: State,
        args: SubscribeArgs[U],
    ) -> tuple[State, SubscriptionResult]:
        """
        state: object that is passed through the entire tree structure
        """

    def discover(self, state: State):
        return state

    def assign_weights(self, state: State, weight: int):
        return state


class SingleChildFlowableNode[U, V](FlowableNode[V]):
    """
    Represents a state monad node with a single child.
    """

    @property
    @abstractmethod
    def child(self) -> FlowableNode[U]: ...

    def discover(self, state: State):
        return self.child.discover(state)

    def assign_weights(self, state: State, weight: int):
        return self.child.assign_weights(state, weight)


class TwoChildrenFlowableNode[L, R, V](FlowableNode[V]):
    """
    Represents a state monad node with two children.
    """

    @property
    @abstractmethod
    def left(self) -> FlowableNode[L]: ...

    @property
    @abstractmethod
    def right(self) -> FlowableNode[R]: ...

    def discover(
        self,
        state: State,
    ):
        state = self.left.discover(state)
        return self.right.discover(state)

    def assign_weights(
        self,
        state: State,
        weight: int,
    ):
        state = self.left.assign_weights(state, weight)
        return self.right.assign_weights(state, weight)


class MultiChildrenFlowableNode[U, V](FlowableNode[V]):
    """
    Represents a state monad node with two children.
    """

    @property
    @abstractmethod
    def children(self) -> tuple[FlowableNode[U], ...]: ...

    def discover(
        self,
        state: State,
    ):
        for child in self.children:
            state = child.discover(state)
        return state

    def assign_weights(
        self,
        state: State,
        weight: int,
    ):
        for child in self.children:
            state = child.assign_weights(state, weight)
        return state
