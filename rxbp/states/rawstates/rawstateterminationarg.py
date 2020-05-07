from abc import ABC, abstractmethod

from rxbp.states.measuredstates.measuredstate import MeasuredState
from rxbp.states.rawstates.rawstate import RawState
from rxbp.states.rawstates.rawterminationstates import RawTerminationStates


class RawStateTerminationArg(RawState, ABC):
    @abstractmethod
    def get_measured_state(self, raw_termination_state: RawTerminationStates.TerminationState) -> MeasuredState:
        ...