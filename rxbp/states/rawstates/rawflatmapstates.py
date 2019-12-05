from abc import ABC
from typing import Optional

from rxbp.states.measuredstates.flatmapstates import FlatMapStates
from rxbp.states.rawstates.rawstatenoargs import RawStateNoArgs


class RawFlatMapStates:
    class State(RawStateNoArgs, ABC):
        pass

    class InitialState(State):
        def get_measured_state(self) -> FlatMapStates.State:
            return FlatMapStates.InitialState()

    class WaitOnOuter(State):
        def __init__(self):
            self.raw_prev_state: Optional['RawFlatMapStates.State'] = None

            self.meas_state: Optional['FlatMapStates.State'] = None

        def get_measured_state(self):
            if self.meas_state is None:
                meas_prev_state = self.raw_prev_state.get_measured_state()

                if any([
                    isinstance(meas_prev_state, FlatMapStates.Stopped),
                    isinstance(meas_prev_state, FlatMapStates.OnOuterException),
                    isinstance(meas_prev_state, FlatMapStates.OnOuterCompleted)
                ]):
                    meas_state = FlatMapStates.Stopped()

                else:
                    meas_state = FlatMapStates.WaitOnOuter()
            else:
                meas_state = self.meas_state

            return meas_state

    class Active(State):
        """ Outer value received, inner observable possibly already subscribed
        """

        def __init__(self):
            self.raw_prev_state: Optional['RawFlatMapStates.State'] = None

            self.meas_state: Optional['FlatMapStates.State'] = None

        def get_measured_state(self):
            if self.meas_state is None:
                meas_prev_state = self.raw_prev_state.get_measured_state()

                if any([
                    isinstance(meas_prev_state, FlatMapStates.Stopped),
                    isinstance(meas_prev_state, FlatMapStates.OnOuterException),
                ]):
                    meas_state = FlatMapStates.Stopped()

                elif isinstance(meas_prev_state, FlatMapStates.OnOuterCompleted):
                    meas_state = meas_prev_state

                else:
                    meas_state = FlatMapStates.Active()
            else:
                meas_state = self.meas_state

            return meas_state

    class OnOuterCompleted(State):
        # outer observer does not complete the output observer (except in WaitOnNextChild); however, it signals
        #   that output observer should be completed if inner observer completes
        def __init__(self):
            self.raw_prev_state: Optional['RawFlatMapStates.State'] = None

            self.meas_state: Optional['FlatMapStates.State'] = None

        def get_measured_state(self):
            if self.meas_state is None:
                meas_prev_state = self.raw_prev_state.get_measured_state()

                if isinstance(meas_prev_state, FlatMapStates.Stopped):
                    meas_state = meas_prev_state

                elif isinstance(meas_prev_state, FlatMapStates.WaitOnOuter):
                    meas_state = FlatMapStates.Stopped()

                else:
                    meas_state = FlatMapStates.OnOuterCompleted()
            else:
                meas_state = self.meas_state

            return meas_state

    class OnOuterException(State):
        def __init__(self, exc: Exception):
            self.exc = exc

            self.raw_prev_state: Optional['RawFlatMapStates.State'] = None

            self.meas_state: Optional['FlatMapStates.State'] = None

        def get_measured_state(self):
            if self.meas_state is None:
                meas_prev_state = self.raw_prev_state.get_measured_state()

                if isinstance(meas_prev_state, FlatMapStates.Stopped):
                    meas_state = meas_prev_state

                elif any([
                    isinstance(meas_prev_state, FlatMapStates.InitialState),
                    isinstance(meas_prev_state, FlatMapStates.WaitOnOuter),
                ]):
                    meas_state = FlatMapStates.Stopped()

                else:
                    meas_state = FlatMapStates.OnOuterException(exc=self.exc)
            else:
                meas_state = self.meas_state

            return meas_state

    class Stopped(State):
        """ either inner observable completed or raise exception
        """
        def get_measured_state(self):
            return self