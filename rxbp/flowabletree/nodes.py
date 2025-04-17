from __future__ import annotations

from abc import ABC, abstractmethod

from rxbp.state import State
from rxbp.flowabletree.subscribeargs import SubscribeArgs
from rxbp.flowabletree.observeresult import ObserveResult


class FlowableNode[V](ABC):
    @abstractmethod
    def unsafe_subscribe(
        self,
        state: State,
        args: SubscribeArgs[V],
    ) -> tuple[State, ObserveResult]:
        """
        state: object that is passed through the entire tree structure
        """

    def discover(
        self, 
        subscriber_count: dict[FlowableNode, int]
    ): # -> dict[FlowableNode, int]:
        ...

    def assign_weights(
        self,
        weight: int,
        shared_weightsss: dict[FlowableNode, int],
        subscriber_count: dict[FlowableNode, int],
    ): # -> dict[FlowableNode, int]:
        ...


class SingleChildFlowableNode[V, ChildV](FlowableNode[V]):
    """
    Represents a state monad node with a single child.
    """

    @property
    @abstractmethod
    def child(self) -> FlowableNode[ChildV]: ...

    def discover(self, subscriber_count: dict[FlowableNode, int]):
        self.child.discover(subscriber_count)

    def assign_weights(
        self,
        weight: int,
        shared_weights: dict[FlowableNode, int],
        subscriber_count: dict[FlowableNode, int],
    ):
        self.child.assign_weights(weight, shared_weights, subscriber_count)


class TwoChildrenFlowableNode[V, L, R](FlowableNode[V]):
    """
    Represents a state monad node with two children.
    """

    @property
    @abstractmethod
    def left(self) -> FlowableNode[L]: ...

    @property
    @abstractmethod
    def right(self) -> FlowableNode[R]: ...

    def discover(self, subscriber_count: dict[FlowableNode, int]):
        self.left.discover(subscriber_count)
        self.right.discover(subscriber_count)

    def assign_weights(
        self,
        weight: int,
        shared_weights: dict[FlowableNode, int],
        subscriber_count: dict[FlowableNode, int],
    ):
        self.left.assign_weights(weight, shared_weights, subscriber_count)
        self.right.assign_weights(weight, shared_weights, subscriber_count)


class MultiChildrenFlowableNode[V](FlowableNode[V]):
    """
    Represents a state monad node with two children.
    """

    @property
    @abstractmethod
    def children(self) -> tuple[FlowableNode, ...]: ...

    def discover(self, subscriber_count: dict[FlowableNode, int]):
        for child in self.children:
            child.discover(subscriber_count)

    def assign_weights(
        self,
        upstream_weight: int,
        weights: dict[FlowableNode, int],
        subscriber_count: dict[FlowableNode, int],
    ):
        for child in self.children:
            child.assign_weights(upstream_weight, weights, subscriber_count)
