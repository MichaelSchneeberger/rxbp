from abc import ABC, abstractmethod

from rxbp.state import State


class AssignWeightMixin(ABC):
    @abstractmethod
    def discover(self, state: State) -> State: ...

    @abstractmethod
    def assign_weights(self, state: State, weight: int) -> State: ...
