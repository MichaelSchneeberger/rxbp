from abc import ABC
from typing import Iterator, Optional, Any

from rxbp.ack.acksubject import AckSubject
from rxbp.ack.mixins.ackmixin import AckMixin
from rxbp.selectors.selectionmsg import SelectionMsg
from rxbp.states.measuredstates.controlledzipstates import ControlledZipStates
from rxbp.states.measuredstates.terminationstates import TerminationStates
from rxbp.states.rawstates.rawstateterminationarg import RawStateTerminationArg
from rxbp.states.rawstates.rawterminationstates import RawTerminationStates


class RawControlledZipStates:

    class ControlledZipState(RawStateTerminationArg, ABC):
        pass

    class WaitOnLeft(ControlledZipState):
        def __init__(
                self,
                right_val: Any,
                right_ack: AckSubject,
                right_iter: Iterator,
                # right_sel: Optional[SelectionMsg],
                # left_sel: Optional[SelectionMsg],
                # right_sel_ack: Optional[AckMixin],
        ):
            self.right_val = right_val
            self.right_ack = right_ack
            self.right_iter = right_iter
            # self.right_sel = right_sel
            # self.left_sel = left_sel
            # self.right_sel_ack = right_sel_ack

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
                    # right_sel=self.right_sel,
                    # left_sel=self.left_sel,
                    # right_sel_ack=self.right_sel_ack,
                )

    class WaitOnRight(ControlledZipState):
        def __init__(
                self,
                left_val: Any,
                left_ack: AckSubject,
                left_iter: Iterator,
                # right_sel: Optional[SelectionMsg],
                # left_sel: Optional[SelectionMsg],
                # left_sel_ack: Optional[AckMixin],
        ):
            self.left_val = left_val
            self.left_iter = left_iter
            self.left_ack = left_ack
            # self.right_sel = right_sel
            # self.left_sel = left_sel
            # self.left_sel_ack = left_sel_ack

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
                    # right_sel=self.right_sel,
                    # left_sel=self.left_sel,
                    # left_sel_ack=self.left_sel_ack,
                )

    class WaitOnLeftRight(ControlledZipState):
        # def __init__(
        #         self,
        #         right_sel: Optional[SelectionMsg],
        #         left_sel: Optional[SelectionMsg],
        # ):
        #     self.right_sel = right_sel
        #     self.left_sel = left_sel

        def get_measured_state(
                self,
                raw_termination_state: RawTerminationStates.TerminationState,
        ):
            termination_state = raw_termination_state.get_measured_state()

            if isinstance(termination_state, TerminationStates.InitState):
                return ControlledZipStates.WaitOnLeftRight(
                    # right_sel=self.right_sel,
                    # left_sel=self.left_sel,
                )
            else:
                return ControlledZipStates.Stopped()

    class ZipElements(ControlledZipState):
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
                            # left_sel_ack=None,
                            # right_sel=prev_state.right_sel,
                            # left_sel=prev_state.left_sel,
                        )
                    else:
                        raw_state = RawControlledZipStates.WaitOnLeft(
                            right_val=self.val,
                            right_iter=self.iter,
                            right_ack=self.ack,
                            # right_sel_ack=None,
                            # right_sel=prev_state.right_sel,
                            # left_sel=prev_state.left_sel,
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
