from abc import ABC, abstractmethod
from typing import Self
from rxbp.flowabletree.observer import Observer
from rxsingle.abc import State as RxSingleState


class State(RxSingleState, ABC):
    pass

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
