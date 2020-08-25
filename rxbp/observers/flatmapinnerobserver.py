import threading
from dataclasses import dataclass
from typing import Optional, List

from rxbp.acknowledgement.acksubject import AckSubject
from rxbp.acknowledgement.continueack import ContinueAck, continue_ack
from rxbp.acknowledgement.single import Single
from rxbp.acknowledgement.stopack import StopAck, stop_ack
from rxbp.observer import Observer
from rxbp.observerinfo import ObserverInfo
from rxbp.observers.connectableobserver import ConnectableObserver
from rxbp.states.measuredstates.flatmapstates import FlatMapStates
from rxbp.states.rawstates.rawflatmapstates import RawFlatMapStates
from rxbp.typing import ElementType


@dataclass
class FlatMapInnerObserver(Observer):
    observer_info: ObserverInfo
    next_conn_observer: Optional[ConnectableObserver]
    outer_upstream_ack: AckSubject
    lock: threading.RLock()
    state: List[RawFlatMapStates.State]

    def on_next(self, elem: ElementType):

        # # for mypy to type check correctly
        # assert isinstance(self.observer_info, ObserverInfo)

        # on_next, on_completed, on_error are called ordered/non-concurrently
        ack = self.observer_info.observer.on_next(elem)

        # if ack==Stop, then update state
        if isinstance(ack, StopAck):
            self.state[0] = RawFlatMapStates.Stopped()
            self.outer_upstream_ack.on_next(ack)

        elif not isinstance(ack, ContinueAck):
            class ResultSingle(Single):
                def on_error(self, exc: Exception):
                    raise NotImplementedError

                def on_next(_, ack):
                    if isinstance(ack, StopAck):
                        self.state[0] = RawFlatMapStates.Stopped()
                        self.outer_upstream_ack.on_next(ack)

            ack.subscribe(ResultSingle())

        return ack

    def on_error(self, err):
        # next_state = RawFlatMapStates.Stopped()
        #
        # with self.lock:
        #     prev_state = self.state[0]
        #     self.state[0] = next_state
        #
        # prev_meas_state = prev_state.get_measured_state()
        #
        # if not isinstance(prev_meas_state, FlatMapStates.Stopped):

        self.observer_info.observer.on_error(err)

        # stop outer stream as well
        self.outer_upstream_ack.on_next(stop_ack)

    def on_completed(self):
        """ on_next* (on_completed | on_error)?
        """

        if self.next_conn_observer is None:
            next_state = RawFlatMapStates.WaitOnOuter()

        else:
            next_state = RawFlatMapStates.Active()

        with self.lock:
            next_state.raw_prev_state = self.state[0]
            self.state[0] = next_state

        prev_meas_state = next_state.raw_prev_state.get_measured_state()
        meas_state = next_state.get_measured_state()

        # possible previous states
        # - Active -> outer on_next call completed before this
        # - WaitOnActiveChild -> outer on_next call completes after this
        # - WaitComplete -> outer.on_complete or outer.on_error was called

        # connect next child observer
        if any([
            isinstance(meas_state, FlatMapStates.Active),
            isinstance(meas_state, FlatMapStates.OnOuterCompleted),
        ]):
            self.next_conn_observer.connect()

        elif isinstance(meas_state, FlatMapStates.WaitOnOuter):
            self.outer_upstream_ack.on_next(continue_ack)

        elif isinstance(meas_state, FlatMapStates.Stopped):

            if isinstance(prev_meas_state, FlatMapStates.OnOuterCompleted):
                self.observer_info.observer.on_completed()
            elif isinstance(prev_meas_state, FlatMapStates.OnOuterException):
                self.observer_info.observer.on_next(prev_meas_state.exc)
            elif isinstance(prev_meas_state, FlatMapStates.Stopped):
                pass
            else:
                raise Exception(f'illegal case "{prev_meas_state}"')

            return

        else:
            raise Exception(f'illegal state "{meas_state}"')