from abc import ABC
from dataclasses import dataclass
from typing import Iterator

from rxbp.ack.acksubject import AckSubject
from rxbp.states.measuredstates.terminationstates import TerminationStates
from rxbp.states.measuredstates.zipstates import ZipStates
from rxbp.states.rawstates.rawstateterminationarg import RawStateTerminationArg
from rxbp.states.rawstates.rawterminationstates import RawTerminationStates


class RawZipStates:

    class ZipState(RawStateTerminationArg, ABC):
        pass

    @dataclass
    class WaitOnLeft(ZipState):
        right_ack: AckSubject
        right_iter: Iterator

        def get_measured_state(self, raw_termination_state: RawTerminationStates.TerminationState):
            termination_state = raw_termination_state.get_measured_state()

            if any([
                isinstance(termination_state, TerminationStates.LeftCompletedState),
                isinstance(termination_state, TerminationStates.ErrorState),
            ]):
                return ZipStates.Stopped()
            else:
                return ZipStates.WaitOnLeft(right_ack=self.right_ack, right_iter=self.right_iter)

    @dataclass
    class WaitOnRight(ZipState):
        left_ack: AckSubject
        left_iter: Iterator

        def get_measured_state(self, raw_termination_state: RawTerminationStates.TerminationState):
            termination_state = raw_termination_state.get_measured_state()

            if any([
                isinstance(termination_state, TerminationStates.RightCompletedState),
                isinstance(termination_state, TerminationStates.ErrorState),
            ]):
                return ZipStates.Stopped()
            else:
                return ZipStates.WaitOnRight(left_ack=self.left_ack, left_iter=self.left_iter)

    class WaitOnLeftRight(ZipState):
        def get_measured_state(self, raw_termination_state: RawTerminationStates.TerminationState):
            termination_state = raw_termination_state.get_measured_state()

            if isinstance(termination_state, TerminationStates.InitState):
                return ZipStates.WaitOnLeftRight()
            else:
                return ZipStates.Stopped()

    class ElementReceived(ZipState):
        def __init__(self, is_left: bool, ack: AckSubject, iter: Iterator):
            self.is_left = is_left
            self.ack = ack
            self.iter = iter

            # to be overwritten synchronously right after initializing the object
            self.prev_raw_state: RawZipStates.ZipState = None
            self.prev_raw_termination_state: RawTerminationStates.TerminationState = None

            self.raw_state: RawZipStates.ZipState = None

        def get_measured_state(self, raw_termination_state: RawTerminationStates.TerminationState):
            if self.raw_state is None:
                prev_state = self.prev_raw_state.get_measured_state(
                    raw_termination_state=self.prev_raw_termination_state)

                if isinstance(prev_state, ZipStates.Stopped):
                    raw_state = self.prev_raw_state

                elif isinstance(prev_state, ZipStates.WaitOnLeftRight):
                    if self.is_left:
                        raw_state = RawZipStates.WaitOnRight(left_ack=self.ack, left_iter=self.iter)
                    else:
                        raw_state = RawZipStates.WaitOnLeft(right_ack=self.ack, right_iter=self.iter)

                elif isinstance(prev_state, ZipStates.WaitOnLeft):
                    raw_state = RawZipStates.ZipElements(
                        left_ack=self.ack, left_iter=self.iter,
                        right_ack=prev_state.right_ack, right_iter=prev_state.right_iter,
                    )

                elif isinstance(prev_state, ZipStates.WaitOnRight):
                    raw_state = RawZipStates.ZipElements(
                        left_ack=prev_state.left_ack, left_iter=prev_state.left_iter,
                        right_ack=self.ack, right_iter=self.iter,
                    )

                else:
                    raise Exception(f'illegal previous state "{prev_state}"')

                self.raw_state = raw_state
            else:
                raw_state = self.raw_state

            return raw_state.get_measured_state(raw_termination_state)

    class ZipElements(ZipStates.ZipElements, ZipState):
        def get_measured_state(self, raw_termination_state: RawTerminationStates.TerminationState):
            return self
