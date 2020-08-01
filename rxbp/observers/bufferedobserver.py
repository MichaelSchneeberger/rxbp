import math
import threading
from abc import ABC, abstractmethod
from dataclasses import dataclass
from queue import Queue
from typing import Optional, List

from rxbp.ack.acksubject import AckSubject
from rxbp.ack.continueack import ContinueAck, continue_ack
from rxbp.ack.mixins.ackmixin import AckMixin
from rxbp.ack.operators.observeon import _observe_on
from rxbp.ack.single import Single
from rxbp.ack.stopack import StopAck, stop_ack
from rxbp.observer import Observer
from rxbp.scheduler import Scheduler
from rxbp.states.measuredstates.measuredstate import MeasuredState
from rxbp.states.rawstates.rawstatenoargs import RawStateNoArgs
from rxbp.states.rawstates.rawterminationstates import RawTerminationStates
from rxbp.typing import ElementType


class BufferedStates:
    class State(MeasuredState):
        pass

    @dataclass
    class WaitingState(State):
        last_ack: Optional[AckSubject]

    class RunningState(State):
        pass

    # class RunningButCompleted(State):
    #     pass

    class Completed(State):
        pass


class RawBufferedStates:
    class State(ABC):
        @abstractmethod
        def get_measured_state(self, has_elements: bool) -> MeasuredState:
            ...

    @dataclass
    class InitialState(State):
        last_ack: AckMixin
        meas_state: Optional[BufferedStates.State]

        def get_measured_state(self, has_elements: bool) -> MeasuredState:
            if self.meas_state is None:
                if has_elements:
                    return BufferedStates.RunningState()

                else:
                    return BufferedStates.WaitingState(self.last_ack)

            return self.meas_state

    @dataclass
    class OnCompleted(State):
        prev_state: Optional['RawBufferedStates.State']
        meas_state: Optional[BufferedStates.State]

        def get_measured_state(self, has_elements: bool) -> MeasuredState:
            if self.meas_state is None:
                prev_meas_state = self.prev_state.get_measured_state(has_elements=has_elements)

                if isinstance(prev_meas_state, BufferedStates.RunningState):
                    self.meas_state = prev_meas_state

                else:
                    self.meas_state = BufferedStates.Completed()

            return self.meas_state

    class OnErrorOrDownStreamStopped(State):
        def get_measured_state(self, has_elements: bool) -> MeasuredState:
            return BufferedStates.Completed()


@dataclass
class BufferedObserver(Observer):
    underlying: Observer
    scheduler: Scheduler
    subscribe_scheduler: Scheduler
    buffer_size: Optional[int]

    def __post_init__(self):
        self.em = self.scheduler.get_execution_model()

        self.lock = threading.RLock()

        self.state: RawBufferedStates.State = RawBufferedStates.InitialState(meas_state=None, last_ack=continue_ack)
        self.queue = []
        self.back_pressure = None

    def _start_loop(self, last_ack: Optional[AckMixin], next_index: int):
        def schedule_ack(ack: AckMixin, next: ElementType):
            outer_self = self

            class ResultSingle(Single):
                def on_next(self, ack: AckMixin):
                    if isinstance(ack, ContinueAck):
                        last_ack = outer_self.underlying.on_next(next)

                        with outer_self.lock:
                            outer_self.queue.pop(0)
                            len_queue = len(outer_self.queue)
                            return_ack = outer_self.back_pressure

                        if len_queue == 0:
                            if isinstance(return_ack, AckSubject):
                                return_ack.on_next(continue_ack)

                            return

                        next_index = outer_self.em.next_frame_index(0)

                        outer_self._start_loop(last_ack=last_ack, next_index=next_index)

                    else:
                        outer_self.state = RawBufferedStates.OnErrorOrDownStreamStopped()

            _observe_on(ack, self.scheduler).subscribe(ResultSingle())

        while True:

            # has elements in the queue
            # if 0 < len(self.queue):

            next = self.queue[0]

            if next_index == 0:

                if isinstance(last_ack, ContinueAck):
                    last_ack = self.underlying.on_next(next)

                    with self.lock:
                        self.queue.pop(0)
                        len_queue = len(self.queue)
                        return_ack = self.back_pressure

                    if len_queue == 0:
                        if isinstance(return_ack, AckSubject):
                            return_ack.on_next(continue_ack)

                        return

                    next_index = self.em.next_frame_index(next_index)

                elif isinstance(last_ack, StopAck):
                    self.state = RawBufferedStates.OnErrorOrDownStreamStopped()
                    return

                else:
                    schedule_ack(last_ack, next=next)
                    return

            # schedule next element from time to time
            else:
                schedule_ack(last_ack, next=next)
                return

            # # no elements in queue
            # else:
            #     pass

    def on_next(self, elem: ElementType):

        if self.back_pressure is None:
            if len(self.queue) < self.buffer_size:
                return_ack = continue_ack

            else:
                return_ack = AckSubject()
                self.back_pressure = return_ack

        else:
            return_ack = self.back_pressure

        with self.lock:
            len_queue = len(self.queue)
            self.queue.append(elem)

        # if meas_back_pressure is None:
        #     if len_queue < self.buffer_size:
        #         return_ack = continue_ack
        #
        #     else:
        #         self.back_pressure = AckSubject()
        #         return_ack = self.back_pressure
        #
        # else:
        #     return_ack = self.back_pressure

        prev_meas_state = self.state.get_measured_state(bool(len_queue))

        if isinstance(prev_meas_state, BufferedStates.WaitingState):
            last_ack = prev_meas_state.last_ack

            self._start_loop(last_ack=last_ack, next_index=1)

            return return_ack

        elif isinstance(prev_meas_state, BufferedStates.RunningState):
            return return_ack

        else:
            return stop_ack

    def on_error(self, exc):
        next_raw_state = RawBufferedStates.OnErrorOrDownStreamStopped()

        with self.lock:
            prev_state = self.state
            self.state = next_raw_state

        prev_meas_state = prev_state.get_measured_state(has_elements=False)

        if not isinstance(prev_meas_state, BufferedStates.Completed):
            self.underlying.on_error(exc)

    def on_completed(self):
        next_raw_state = RawBufferedStates.OnCompleted(
            prev_state=None,
            meas_state=None,
        )

        with self.lock:
            next_raw_state.prev_state = self.state
            self.state = next_raw_state
            len_queue = len(self.queue)

        prev_meas_state = next_raw_state.prev_state.get_measured_state(bool(len_queue))
        curr_meas_state = next_raw_state.get_measured_state(bool(len_queue))

        if not isinstance(prev_meas_state, BufferedStates.Completed) and \
                isinstance(curr_meas_state, BufferedStates.Completed):
            self.underlying.on_completed()
