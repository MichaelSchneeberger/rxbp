import threading

from rx.disposable import CompositeDisposable

from rxbp.ack.acksubject import AckSubject
from rxbp.ack.continueack import ContinueAck, continue_ack
from rxbp.ack.mixins.ackmixin import AckMixin
from rxbp.ack.single import Single
from rxbp.ack.stopack import stop_ack
from rxbp.observable import Observable
from rxbp.observer import Observer
from rxbp.observerinfo import ObserverInfo
from rxbp.states.measuredstates.mergestates import MergeStates
from rxbp.states.rawstates.rawmergestates import RawMergeStates
from rxbp.states.rawstates.rawterminationstates import RawTerminationStates
from rxbp.typing import ElementType


class MergeObservable(Observable):
    """ Merges the elements of two Flowables into a single Flowable

        s1.merge(s2).subscribe(o, scheduler=s)

    ^ callstack
    |
    |       o
    |      /
    |   merge  merge     o
    |    /      /       /
    |   s1     s2     ack
    |  /      /       /
    | s      s      ...                           time
    --------------------------------------------->

    ack: async acknowledgment returned by observer `o`

    Scenario 1: Send left, right arrives, out_ack returns Continue, send right, send left (preferred)
    Scenario 2: Send left, right arrives, out_ack returns Continue, send left, send right
    """

    def __init__(
            self,
            left: Observable,
            right: Observable,
    ):
        """
        :param left: Flowable whose elements get merged
        :param right: other Flowable whose elements get merged
        """

        self.left = left
        self.right = right

        # MergeObservable states
        self.observer = None
        self.termination_state = RawTerminationStates.InitState()
        self.state = RawMergeStates.NoneReceived()

        self.lock = threading.RLock()

    class ResultSingle(Single):
        def __init__(self, source: 'MergeObservable'):
            self.source = source

        def on_next(self, ack: AckMixin):
            if isinstance(ack, ContinueAck):
                next_state = RawMergeStates.OnAckReceived()

                with self.source.lock:
                    next_state.prev_raw_state = self.source.state
                    next_state.prev_raw_termination_state = self.source.termination_state
                    self.source.state = next_state

                raw_termination_state = next_state.prev_raw_termination_state
                meas_prev_state = next_state.prev_raw_state.get_measured_state(raw_termination_state)
                meas_state = next_state.get_measured_state(raw_termination_state)

                # acknowledgment already sent to left and right
                if isinstance(meas_state, MergeStates.NoneReceived):
                    pass

                # send buffered element and request new one
                elif isinstance(meas_state, MergeStates.NoneReceivedWaitAck):

                    if isinstance(meas_prev_state, MergeStates.SingleReceived):
                        ack = self.source.observer.on_next(meas_prev_state.elem)
                        ack.subscribe(self)
                        meas_prev_state.ack.on_next(continue_ack)

                    else:
                        raise Exception(f'illegal previous state "{meas_prev_state}"')

                # send first buffered element and request new one
                elif isinstance(meas_state, MergeStates.SingleReceived):

                    # previous state indicated that the next element sent is from left
                    if isinstance(meas_prev_state, MergeStates.BothReceivedContinueLeft):
                        ack = self.source.observer.on_next(meas_prev_state.left_elem)
                        ack.subscribe(self)
                        meas_prev_state.left_ack.on_next(continue_ack)

                    elif isinstance(meas_prev_state, MergeStates.BothReceivedContinueRight):
                        ack = self.source.observer.on_next(meas_prev_state.right_elem)
                        ack.subscribe(self)
                        meas_prev_state.right_ack.on_next(continue_ack)

                    else:
                        raise Exception(f'illegal previous state "{meas_prev_state}"')

                elif isinstance(meas_state, MergeStates.Stopped):
                    if isinstance(meas_prev_state, MergeStates.SingleReceived):
                        self.source.observer.on_next(meas_prev_state.elem)
                        self.source.observer.on_completed()

                    else:
                        pass

                else:
                    raise Exception(f'illegal state "{meas_state}"')

        def on_error(self, exc: Exception):
            raise NotImplementedError

    def _on_next(self, next_state: RawMergeStates.ElementReceivedBase):
        with self.lock:
            next_state.prev_raw_state = self.state
            next_state.prev_raw_termination_state = self.termination_state
            self.state = next_state

        raw_termination_state = next_state.prev_raw_termination_state
        meas_prev_state = next_state.prev_raw_state.get_measured_state(raw_termination_state)
        meas_state = next_state.get_measured_state(raw_termination_state)

        # left is first
        if isinstance(meas_state, MergeStates.NoneReceivedWaitAck):

            # send element
            out_ack = self.observer.on_next(next_state.elem)

            out_ack.subscribe(MergeObservable.ResultSingle(source=self))

            return continue_ack

        elif isinstance(meas_state, MergeStates.SingleReceived):
            pass

        elif isinstance(meas_state, MergeStates.BothReceived):
            pass

        # keep waiting for right and acknowledment
        elif isinstance(meas_state, MergeStates.Stopped):
            if isinstance(meas_prev_state, MergeStates.Stopped):
                self.observer.on_completed()
                return stop_ack
            else:
                pass

        else:
            raise Exception(f'illegal state "{meas_state}"')

        return next_state.ack

    def _signal_on_complete_or_on_error(self, prev_state: MergeStates.MergeState, exc: Exception = None):
        """ this function is called once

        :param raw_state:
        :param exc:
        :return:
        """

        # stop active acknowledgments
        if isinstance(prev_state, MergeStates.NoneReceivedWaitAck):
            pass
        elif isinstance(prev_state, MergeStates.SingleReceived):
            prev_state.ack.on_next(stop_ack)
        else:
            pass

        # terminate observer
        if exc:
            self.observer.on_error(exc)
        else:
            self.observer.on_completed()

    def _on_error_or_complete(self, next_termination_state: RawTerminationStates.TerminationState, exc: Exception = None):
        with self.lock:
            next_termination_state.raw_prev_state = self.termination_state
            self.termination_state = next_termination_state

            raw_state = self.state

        prev_raw_termination_state = next_termination_state.raw_prev_state
        raw_termination_state = next_termination_state

        meas_prev_state = raw_state.get_measured_state(prev_raw_termination_state)
        meas_state = raw_state.get_measured_state(raw_termination_state)

        if not isinstance(meas_prev_state, MergeStates.Stopped) \
                and isinstance(meas_state, MergeStates.Stopped):
            self._signal_on_complete_or_on_error(meas_prev_state, exc)

    def _on_error(self, exc: Exception):
        termination_state = RawTerminationStates.ErrorState(exc)

        self._on_error_or_complete(next_termination_state=termination_state, exc=exc)

    def _on_completed_left(self):
        termination_state = RawTerminationStates.LeftCompletedState()

        self._on_error_or_complete(next_termination_state=termination_state)

    def _on_completed_right(self):
        termination_state = RawTerminationStates.RightCompletedState()

        self._on_error_or_complete(next_termination_state=termination_state)

    def observe(self, observer_info: ObserverInfo):
        self.observer = observer_info.observer
        source = self

        class LeftObserver(Observer):
            def on_next(self, elem: ElementType):
                next_state = RawMergeStates.OnLeftReceived(elem=elem, ack=AckSubject())
                return source._on_next(next_state)

            def on_error(self, exc):
                source._on_error(exc)

            def on_completed(self):
                source._on_completed_left()

        class RightObserver(Observer):
            def on_next(self, elem: ElementType):
                next_state = RawMergeStates.OnRightReceived(elem=elem, ack=AckSubject())
                return source._on_next(next_state)

            def on_error(self, exc):
                source._on_error(exc)

            def on_completed(self):
                source._on_completed_right()

        left_observer = LeftObserver()
        left_subscription = observer_info.copy(left_observer)
        d1 = self.left.observe(left_subscription)

        right_observer = RightObserver()
        right_subscription = observer_info.copy(right_observer)
        d2 = self.right.observe(right_subscription)

        return CompositeDisposable(d1, d2)
