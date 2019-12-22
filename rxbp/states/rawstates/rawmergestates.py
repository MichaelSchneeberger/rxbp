from abc import ABC

from rxbp.ack.acksubject import AckSubject
from rxbp.states.measuredstates.mergestates import MergeStates
from rxbp.states.measuredstates.terminationstates import TerminationStates
from rxbp.states.rawstates.rawstateterminationarg import RawStateTerminationArg
from rxbp.states.rawstates.rawterminationstates import RawTerminationStates
from rxbp.typing import ElementType


class RawMergeStates:
    class MergeState(RawStateTerminationArg, ABC):
        pass

    class NoneReceived(MergeState):
        def get_measured_state(self, raw_termination_state: RawTerminationStates.TerminationState):
            meas_termination_state = raw_termination_state.get_measured_state()

            if any([
                isinstance(meas_termination_state, TerminationStates.ErrorState),
                isinstance(meas_termination_state, TerminationStates.BothCompletedState),
            ]):
                return MergeStates.Stopped()
            else:
                return MergeStates.NoneReceived()

    class NoneReceivedWaitAck(MergeState):
        def get_measured_state(self, raw_termination_state: RawTerminationStates.TerminationState):
            meas_termination_state = raw_termination_state.get_measured_state()

            if any([
                isinstance(meas_termination_state, TerminationStates.BothCompletedState),
                isinstance(meas_termination_state, TerminationStates.ErrorState),
            ]):
                return MergeStates.Stopped()
            else:
                return MergeStates.NoneReceivedWaitAck()

    class SingleReceived(MergeState, ABC):
        def __init__(self, elem: ElementType, ack: AckSubject):
            self.elem = elem
            self.ack = ack

    class LeftReceived(SingleReceived):
        def get_measured_state(self, raw_termination_state: RawTerminationStates.TerminationState):
            termination_state = raw_termination_state.get_measured_state()

            if isinstance(termination_state, TerminationStates.ErrorState):
                return MergeStates.Stopped()
            else:
                return MergeStates.LeftReceived(elem=self.elem, ack=self.ack)

    class RightReceived(SingleReceived):
        def get_measured_state(self, raw_termination_state: RawTerminationStates.TerminationState):
            termination_state = raw_termination_state.get_measured_state()

            if isinstance(termination_state, TerminationStates.ErrorState):
                return MergeStates.Stopped()
            else:
                return MergeStates.RightReceived(elem=self.elem, ack=self.ack)

    class BothReceived(MergeState, ABC):
        def __init__(
                self,
                left_elem: ElementType,
                right_elem: ElementType,
                left_ack: AckSubject,
                right_ack: AckSubject,
        ):
            self.left_elem = left_elem
            self.right_elem = right_elem
            self.left_ack = left_ack
            self.right_ack = right_ack

    class BothReceivedContinueLeft(BothReceived):
        def get_measured_state(self, raw_termination_state: RawTerminationStates.TerminationState):
            termination_state = raw_termination_state.get_measured_state()

            if isinstance(termination_state, TerminationStates.ErrorState):
                return MergeStates.Stopped()
            else:
                return MergeStates.BothReceivedContinueLeft(left_elem=self.left_elem, right_elem=self.right_elem, left_ack=self.left_ack,
                                                right_ack=self.right_ack)

    class BothReceivedContinueRight(BothReceived):
        def get_measured_state(self, raw_termination_state: RawTerminationStates.TerminationState):
            termination_state = raw_termination_state.get_measured_state()

            if isinstance(termination_state, TerminationStates.ErrorState):
                return MergeStates.Stopped()
            else:
                return MergeStates.BothReceivedContinueRight(left_elem=self.left_elem, right_elem=self.right_elem, left_ack=self.left_ack,
                                                right_ack=self.right_ack)

    class ElementReceivedBase(MergeState, ABC):
        def __init__(self, elem: ElementType, ack: AckSubject):
            self.elem = elem
            self.ack = ack

            self.prev_raw_termination_state: RawTerminationStates.TerminationState = None
            self.prev_raw_state: 'RawMergeStates.MergeState' = None

            self.raw_state: MergeStates.MergeState = None

    class OnLeftReceived(ElementReceivedBase):
        def get_measured_state(self, raw_termination_state: RawTerminationStates.TerminationState):
            if self.raw_state is None:
                meas_prev_state = self.prev_raw_state.get_measured_state(self.prev_raw_termination_state)

                if isinstance(meas_prev_state, MergeStates.NoneReceived):
                    raw_state = RawMergeStates.NoneReceivedWaitAck()

                elif isinstance(meas_prev_state, MergeStates.NoneReceivedWaitAck):
                    raw_state = RawMergeStates.LeftReceived(
                        elem=self.elem,
                        ack=self.ack,
                    )

                elif isinstance(meas_prev_state, MergeStates.RightReceived):
                    raw_state = RawMergeStates.BothReceivedContinueRight(
                        left_elem=self.elem,
                        right_elem=meas_prev_state.elem,
                        left_ack=self.ack,
                        right_ack=meas_prev_state.ack,
                    )

                elif isinstance(meas_prev_state, MergeStates.Stopped):
                    raw_state = meas_prev_state

                else:
                    raise Exception(f'illegal state "{meas_prev_state}"')

                self.raw_state = raw_state
            else:
                raw_state = self.raw_state

            return raw_state.get_measured_state(raw_termination_state)

    class OnRightReceived(ElementReceivedBase):
        def get_measured_state(self, raw_termination_state: RawTerminationStates.TerminationState):
            if self.raw_state is None:
                meas_prev_state = self.prev_raw_state.get_measured_state(self.prev_raw_termination_state)

                if isinstance(meas_prev_state, MergeStates.NoneReceived):
                    raw_state = RawMergeStates.NoneReceivedWaitAck()

                elif isinstance(meas_prev_state, MergeStates.NoneReceivedWaitAck):
                    raw_state = RawMergeStates.RightReceived(
                        elem=self.elem,
                        ack=self.ack,
                    )

                elif isinstance(meas_prev_state, MergeStates.LeftReceived):
                    raw_state = RawMergeStates.BothReceivedContinueLeft(
                        left_elem=meas_prev_state.elem,
                        right_elem=self.elem,
                        left_ack=meas_prev_state.ack,
                        right_ack=self.ack,
                    )

                elif isinstance(meas_prev_state, MergeStates.Stopped):
                    raw_state = self.prev_raw_state

                else:
                    raise Exception(f'illegal state "{meas_prev_state}"')

                self.raw_state = raw_state
            else:
                raw_state = self.raw_state

            return raw_state.get_measured_state(raw_termination_state)

    class OnAckReceived(MergeState):
        def __init__(self):

            self.prev_raw_termination_state: RawTerminationStates.TerminationState = None
            self.prev_raw_state: 'RawMergeStates.MergeState' = None

            self.raw_state: RawMergeStates.MergeState = None

        def get_measured_state(self, raw_termination_state: RawTerminationStates.TerminationState):
            if self.raw_state is None:
                meas_prev_state = self.prev_raw_state.get_measured_state(self.prev_raw_termination_state)

                if isinstance(meas_prev_state, MergeStates.NoneReceivedWaitAck):
                    raw_state = RawMergeStates.NoneReceived()

                elif isinstance(meas_prev_state, MergeStates.SingleReceived):
                    raw_state = RawMergeStates.NoneReceivedWaitAck()

                elif isinstance(meas_prev_state, MergeStates.BothReceivedContinueLeft):
                    raw_state = RawMergeStates.RightReceived(
                        elem=meas_prev_state.right_elem,
                        ack=meas_prev_state.right_ack,
                    )

                elif isinstance(meas_prev_state, MergeStates.BothReceivedContinueRight):
                    raw_state = RawMergeStates.LeftReceived(
                        elem=meas_prev_state.left_elem,
                        ack=meas_prev_state.left_ack,
                    )

                elif isinstance(meas_prev_state, MergeStates.Stopped):
                    raw_state = self.prev_raw_state

                else:
                    raise Exception(f'illegal state "{meas_prev_state}"')

                self.raw_state = raw_state
            else:
                raw_state = self.raw_state

            return raw_state.get_measured_state(raw_termination_state)
