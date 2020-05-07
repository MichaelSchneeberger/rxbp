from abc import ABC, abstractmethod

from rxbp.states.measuredstates.measuredstate import MeasuredState
from rxbp.states.rawstates.rawstate import RawState


class RawStateNoArgs(RawState, ABC):
    @abstractmethod
    def get_measured_state(self) -> MeasuredState:
        ...
