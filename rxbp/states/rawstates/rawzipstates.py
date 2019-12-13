from abc import ABC
from typing import Iterator

from rxbp.ack.acksubject import AckSubject
from rxbp.states.measuredstates.terminationstates import TerminationStates
from rxbp.states.measuredstates.zipstates import ZipStates
from rxbp.states.rawstates.rawstateterminationarg import RawStateTerminationArg
from rxbp.states.rawstates.rawterminationstates import RawTerminationStates


class RawZipStates:

    class ZipState(RawStateTerminationArg, ABC):
        pass

    class WaitOnLeft(ZipState):
        def __init__(
                self,
                right_ack: AckSubject,
                right_iter: Iterator,
        ):
            self.right_ack = right_ack
            self.right_iter = right_iter

        def get_measured_state(self, raw_termination_state: RawTerminationStates.TerminationState):
            termination_state = raw_termination_state.get_measured_state()

            if any([
                isinstance(termination_state, TerminationStates.LeftCompletedState),
                isinstance(termination_state, TerminationStates.ErrorState),
            ]):
                return ZipStates.Stopped()
            else:
                return ZipStates.WaitOnLeft(right_ack=self.right_ack, right_iter=self.right_iter)

    class WaitOnRight(ZipState):
        def __init__(
                self,
                left_ack: AckSubject,
                left_iter: Iterator,
        ):
            self.left_iter = left_iter
            self.left_ack = left_ack

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

    class ZipElements(ZipState):
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

                # Needed for `signal_on_complete_or_on_error`
                elif isinstance(prev_state, ZipStates.WaitOnLeftRight):
                    if self.is_left:
                        raw_state = RawZipStates.WaitOnRight(left_ack=self.ack, left_iter=self.iter)
                    else:
                        raw_state = RawZipStates.WaitOnLeft(right_ack=self.ack, right_iter=self.iter)

                else:
                    raw_state = RawZipStates.ZipElements(
                        is_left=self.is_left, ack=self.ack, iter=self.iter)

                self.raw_state = raw_state
            else:
                raw_state = self.raw_state

            return raw_state.get_measured_state(raw_termination_state)

    # class Stopped(ZipState):
    #     def get_measured_state(self, raw_termination_state: RawTerminationStates.TerminationState):
    #         return ZipStates.Stopped()
