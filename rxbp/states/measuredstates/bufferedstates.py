from dataclasses import dataclass
from typing import Optional

from rxbp.ack.acksubject import AckSubject
from rxbp.states.measuredstates.measuredstate import MeasuredState


class BufferedStates:
    class State(MeasuredState):
        pass

    @dataclass
    class WaitingState(State):
        last_ack: Optional[AckSubject]

    class RunningState(State):
        pass

    class Completed(State):
        pass