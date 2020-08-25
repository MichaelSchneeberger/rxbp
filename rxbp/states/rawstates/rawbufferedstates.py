from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional

from rxbp.acknowledgement.ack import Ack
from rxbp.states.measuredstates.bufferedstates import BufferedStates


class RawBufferedStates:
    class State(ABC):
        @abstractmethod
        def get_measured_state(self, has_elements: bool) -> BufferedStates.State:
            ...

    @dataclass
    class InitialState(State):
        last_ack: Ack
        meas_state: Optional[BufferedStates.State]

        def get_measured_state(self, has_elements: bool) -> BufferedStates.State:
            if self.meas_state is None:
                if has_elements:
                    return BufferedStates.RunningState()

                else:
                    return BufferedStates.WaitingState(self.last_ack)

            return self.meas_state

    @dataclass
    class OnCompleted(State):
        prev_state: Optional['RawBufferedStates.State']

        def get_measured_state(self, has_elements: bool) -> BufferedStates.State:
            prev_meas_state = self.prev_state.get_measured_state(has_elements=has_elements)

            if isinstance(prev_meas_state, BufferedStates.RunningState):
                meas_state = prev_meas_state

            else:
                meas_state = BufferedStates.Completed()

            return meas_state

    class OnErrorOrDownStreamStopped(State):
        def get_measured_state(self, has_elements: bool) -> BufferedStates.State:
            return BufferedStates.Completed()