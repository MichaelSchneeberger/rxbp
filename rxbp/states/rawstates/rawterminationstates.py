from abc import ABC, abstractmethod
from typing import Optional

from rxbp.states.measuredstates.terminationstates import TerminationStates
from rxbp.states.rawstates.rawstatenoargs import RawStateNoArgs


class RawTerminationStates:
    class TerminationState(RawStateNoArgs, ABC):
        @abstractmethod
        def get_measured_state(self) -> TerminationStates.TerminationState:
            ...

    class InitState(TerminationState):
        def get_measured_state(self):
            return TerminationStates.InitState()

    class LeftCompletedState(TerminationState):
        def __init__(self):
            self.raw_prev_state: Optional['RawTerminationStates.TerminationState'] = None

        def get_measured_state(self) -> TerminationStates.TerminationState:
            prev_state = self.raw_prev_state.get_measured_state()

            if isinstance(prev_state, TerminationStates.InitState):
                return TerminationStates.LeftCompletedState()
            if isinstance(prev_state, TerminationStates.RightCompletedState):
                return TerminationStates.BothCompletedState()
            else:
                return prev_state

    class RightCompletedState(TerminationState):
        def __init__(self):
            self.raw_prev_state: Optional['RawTerminationStates.TerminationState'] = None

        def get_measured_state(self) -> TerminationStates.TerminationState:
            prev_state = self.raw_prev_state.get_measured_state()

            if isinstance(prev_state, TerminationStates.InitState):
                return TerminationStates.RightCompletedState()
            if isinstance(prev_state, TerminationStates.LeftCompletedState):
                return TerminationStates.BothCompletedState()
            else:
                return prev_state

    class BothCompletedState(TerminationState):
        def get_measured_state(self):
            return TerminationStates.BothCompletedState()

    class ErrorState(TerminationState):
        def __init__(self, exc: Exception):
            self.exc = exc

        def get_measured_state(self) -> TerminationStates.TerminationState:
            return TerminationStates.ErrorState(exc=self.exc)
