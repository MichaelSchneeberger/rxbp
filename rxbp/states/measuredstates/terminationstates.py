from abc import ABC

from rxbp.states.measuredstates.measuredstate import MeasuredState


class TerminationStates:
    class TerminationState(MeasuredState, ABC):
        pass

    class InitState(TerminationState):
        pass

    class LeftCompletedState(TerminationState, ABC):
        pass

    class RightCompletedState(TerminationState, ABC):
        pass

    class BothCompletedState(LeftCompletedState, RightCompletedState):
        pass

    class ErrorState(TerminationState):
        def __init__(self, exc: Exception):
            self.exc = exc
