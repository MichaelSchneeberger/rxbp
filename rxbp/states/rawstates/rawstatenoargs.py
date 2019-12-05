from abc import ABC, abstractmethod

from rxbp.states.measuredstates.measuredstate import MeasuredState


class RawStateNoArgs(ABC):
    @abstractmethod
    def get_measured_state(self) -> MeasuredState:
        ...