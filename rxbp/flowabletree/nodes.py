from abc import ABC, abstractmethod

from rxbp.flowabletree.data.observer import Observer
from rxbp.flowabletree.data.observeresult import ObserveResult
from rxbp.state import State


class FlowableNode[V](ABC):
    @abstractmethod
    def unsafe_subscribe(
        self, state: State, observer: Observer[V],
    ) -> tuple[State, ObserveResult]:
        """
        state: object that is passed through the entire tree structure
        """


class SingleChildFlowableNode[V, ChildV](FlowableNode[V]):
    """
    Represents a state monad node with a single child.
    """

    @property
    @abstractmethod
    def child(self) -> FlowableNode[ChildV]: ...


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


class MultiChildrenFlowableNode[V](FlowableNode[V]):
    """
    Represents a state monad node with two children.
    """

    @property
    @abstractmethod
    def children(self) -> tuple[FlowableNode, ...]: ...
