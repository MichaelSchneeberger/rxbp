from abc import ABC, abstractmethod
from typing import Self
from rxbp.flowabletree.observer import Observer
from rxsingle.abc import State as RxSingleState


class State(RxSingleState, ABC):
    @property
    @abstractmethod
    def p_rxbp_observer(  # pyright: ignore[reportUnknownParameterType]
            self,
    ) -> Observer | None:  # pyright: ignore[reportMissingTypeArgument]
        ...

    @abstractmethod
    def update_rxbp_observer(self, observer: Observer | None) -> Self:  # pyright: ignore[reportMissingTypeArgument, reportUnknownParameterType]
        ...



    # @property
    # @abstractmethod
    # def connections(
    #         self,
    # ) -> dict[ConnectableFlowableNode[S, U], FlowableNode[S, U]]:
    #     """
    #     Maps connectable Flowable nodes to its source.
    #
    #     This dictionary needs to be provided when subscribing to a Flowable.
    #     """
