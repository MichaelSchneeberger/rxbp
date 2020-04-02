from abc import ABC
from dataclasses import dataclass
from typing import Iterator, Any

from rxbp.ack.acksubject import AckSubject
from rxbp.states.measuredstates.controlledzipstates import ControlledZipStates
from rxbp.states.measuredstates.terminationstates import TerminationStates
from rxbp.states.rawstates.rawstateterminationarg import RawStateTerminationArg
from rxbp.states.rawstates.rawterminationstates import RawTerminationStates


class RawControlledZipStates:

    class ControlledZipState(RawStateTerminationArg, ABC):
        pass

    @dataclass
    class WaitOnLeft(ControlledZipState):
        right_val: Any
        right_ack: AckSubject
        right_iter: Iterator

        def get_measured_state(self, raw_termination_state: RawTerminationStates.TerminationState):
            termination_state = raw_termination_state.get_measured_state()

            if any([
                isinstance(termination_state, TerminationStates.LeftCompletedState),
                isinstance(termination_state, TerminationStates.ErrorState),
            ]):
                return ControlledZipStates.Stopped()
            else:
                return ControlledZipStates.WaitOnLeft(
                    right_val=self.right_val,
                    right_ack=self.right_ack,
                    right_iter=self.right_iter,
                )

    @dataclass
    class WaitOnRight(ControlledZipState):
        left_val: Any
        left_ack: AckSubject
        left_iter: Iterator

        def get_measured_state(
                self,
                raw_termination_state: RawTerminationStates.TerminationState,
        ):
            termination_state = raw_termination_state.get_measured_state()

            if any([
                isinstance(termination_state, TerminationStates.RightCompletedState),
                isinstance(termination_state, TerminationStates.ErrorState),
            ]):
                return ControlledZipStates.Stopped()
            else:
                return ControlledZipStates.WaitOnRight(
                    left_val=self.left_val,
                    left_ack=self.left_ack,
                    left_iter=self.left_iter,
                )

    class WaitOnLeftRight(ControlledZipState):
        def get_measured_state(
                self,
                raw_termination_state: RawTerminationStates.TerminationState,
        ):
            termination_state = raw_termination_state.get_measured_state()

            if isinstance(termination_state, TerminationStates.InitState):
                return ControlledZipStates.WaitOnLeftRight()
            else:
                return ControlledZipStates.Stopped()

    class ElementReceived(ControlledZipState):
        def __init__(
                self,
                is_left: bool,
                val: Any,
                ack: AckSubject,
                iter: Iterator,
        ):
            self.val = val
            self.is_left = is_left
            self.ack = ack
            self.iter = iter

            # to be overwritten synchronously right after initializing the object
            self.prev_raw_state: RawControlledZipStates.ControlledZipState = None
            self.prev_raw_termination_state: RawTerminationStates.TerminationState = None

            self.raw_state: RawControlledZipStates.ControlledZipState = None

        def get_measured_state(
                self,
                raw_termination_state: RawTerminationStates.TerminationState,
        ):

            if self.raw_state is None:
                prev_state = self.prev_raw_state.get_measured_state(
                    raw_termination_state=self.prev_raw_termination_state)

                if isinstance(prev_state, ControlledZipStates.Stopped):
                    raw_state = self.prev_raw_state

                # Needed for `signal_on_complete_or_on_error`
                elif isinstance(prev_state, ControlledZipStates.WaitOnLeftRight):
                    if self.is_left:
                        raw_state = RawControlledZipStates.WaitOnRight(
                            left_val=self.val,
                            left_iter=self.iter,
                            left_ack=self.ack,
                        )
                    else:
                        raw_state = RawControlledZipStates.WaitOnLeft(
                            right_val=self.val,
                            right_iter=self.iter,
                            right_ack=self.ack,
                        )

                else:
                    raw_state = RawControlledZipStates.ZipElements(
                        is_left=self.is_left,
                        val=self.val,
                        iter=self.iter,
                        ack=self.ack,
                    )

                self.raw_state = raw_state
            else:
                raw_state = self.raw_state

            return raw_state.get_measured_state(raw_termination_state)

    class ZipElements(ControlledZipStates.ZipElements, ControlledZipState):
        def get_measured_state(self, raw_termination_state: RawTerminationStates.TerminationState):
            return self
