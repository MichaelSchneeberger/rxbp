import threading
from abc import ABC, abstractmethod
from typing import Callable, Generator, Optional

from rx.disposable import CompositeDisposable
from rxbp.ack.ackimpl import Continue, continue_ack, stop_ack
from rxbp.ack.ackbase import AckBase
from rxbp.ack.acksubject import AckSubject
from rxbp.ack.single import Single

from rxbp.observable import Observable
from rxbp.observer import Observer
from rxbp.observerinfo import ObserverInfo
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

    class TerminationState(ABC):
        @abstractmethod
        def get_measured_state(self):
            ...

    class InitTerminationState(TerminationState):
        def get_measured_state(self):
            return self

    class LeftCompletedState(TerminationState, ABC):
        pass

    class RightCompletedState(TerminationState, ABC):
        pass

    class BothCompletedState(LeftCompletedState, RightCompletedState):
        def get_measured_state(self):
            return self

    class LeftCompletedStateImpl(LeftCompletedState):
        def __init__(self):
            self.raw_prev_state: Optional['MergeObservable.TerminationState'] = None

        def get_measured_state(self):
            prev_state = self.raw_prev_state

            if isinstance(prev_state, MergeObservable.InitTerminationState):
                return self
            if isinstance(prev_state, MergeObservable.RightCompletedState):
                return MergeObservable.BothCompletedState()
            else:
                return self.raw_prev_state

    class RightCompletedStateImpl(RightCompletedState):
        def __init__(self):
            self.raw_prev_state: Optional['MergeObservable.TerminationState'] = None

        def get_measured_state(self):
            prev_state = self.raw_prev_state

            if isinstance(prev_state, MergeObservable.InitTerminationState):
                return self
            if isinstance(prev_state, MergeObservable.LeftCompletedState):
                return MergeObservable.BothCompletedState()
            else:
                return self.raw_prev_state

    class ErrorState(TerminationState):
        def __init__(self, ex: Exception):
            self.ex = ex

        def get_measured_state(self):
            return self

    class MergeState(ABC):
        @abstractmethod
        def get_measured_state(self, termination_state: 'MergeObservable.TerminationState'):
            ...

    class NoneReceived(MergeState):
        def get_measured_state(self, termination_state: 'MergeObservable.TerminationState'):
            meas_termination_state = termination_state.get_measured_state()

            if isinstance(meas_termination_state, MergeObservable.ErrorState) or isinstance(meas_termination_state, MergeObservable.BothCompletedState):
                return MergeObservable.Stopped()
            else:
                return self

    class NoneReceivedWaitAck(MergeState):
        def get_measured_state(self, termination_state: 'MergeObservable.TerminationState'):
            meas_termination_state = termination_state.get_measured_state()
            if isinstance(meas_termination_state, MergeObservable.BothCompletedState) or isinstance(meas_termination_state, MergeObservable.ErrorState):
                return MergeObservable.Stopped()
            else:
                return self

    class SingleReceived(MergeState, ABC):
        def __init__(self, elem: ElementType, ack: AckSubject):
            self.elem = elem
            self.ack = ack

    class LeftReceived(SingleReceived):
        def get_measured_state(self, termination_state: 'MergeObservable.TerminationState'):
            if isinstance(termination_state, MergeObservable.ErrorState):
                return MergeObservable.Stopped()
            else:
                return self

    class RightReceived(SingleReceived):
        def get_measured_state(self, termination_state: 'MergeObservable.TerminationState'):
            if isinstance(termination_state, MergeObservable.ErrorState):
                return MergeObservable.Stopped()
            else:
                return self

    class BothReceived(MergeState, ABC):
        def __init__(
                self,
                left_elem: ElementType,
                right_elem: ElementType,
                left_ack: AckBase,
                right_ack: AckBase,
        ):
            self.left_elem = left_elem
            self.right_elem = right_elem
            self.left_ack = left_ack
            self.right_ack = right_ack

        def get_measured_state(self, termination_state: 'MergeObservable.TerminationState'):
            if isinstance(termination_state, MergeObservable.ErrorState):
                return MergeObservable.Stopped()
            else:
                return self

    class BothReceivedContinueLeft(BothReceived):
        pass

    class BothReceivedContinueRight(BothReceived):
        pass

    class ElementReceivedBase(MergeState, ABC):
        def __init__(self, elem: ElementType, ack: AckSubject):
            self.elem = elem
            self.ack = ack

            self.raw_prev_termination_state: 'MergeObservable.TerminationState' = None
            self.raw_prev_state: 'MergeObservable.MergeState' = None

            self.meas_state: 'MergeObservable.MergeState' = None

    class OnLeftReceived(ElementReceivedBase):
        def get_measured_state(self, termination_state: 'MergeObservable.TerminationState'):
            if self.meas_state is None:
                meas_prev_state = self.raw_prev_state.get_measured_state(self.raw_prev_termination_state)

                if isinstance(meas_prev_state, MergeObservable.NoneReceived):
                    meas_state = MergeObservable.NoneReceivedWaitAck()

                elif isinstance(meas_prev_state, MergeObservable.NoneReceivedWaitAck):
                    meas_state = MergeObservable.LeftReceived(
                        elem=self.elem,
                        ack=self.ack,
                    )

                elif isinstance(meas_prev_state, MergeObservable.RightReceived):
                    meas_state = MergeObservable.BothReceivedContinueRight(
                        left_elem=self.elem,
                        right_elem=meas_prev_state.elem,
                        left_ack=self.ack,
                        right_ack=meas_prev_state.ack,
                    )

                elif isinstance(meas_prev_state, MergeObservable.Stopped):
                    meas_state = meas_prev_state

                else:
                    raise Exception(f'illegal state "{meas_prev_state}"')

                self.meas_state = meas_state
            else:
                meas_state = self.meas_state

            return meas_state.get_measured_state(termination_state)

    class OnRightReceived(ElementReceivedBase):
        def get_measured_state(self, termination_state: 'MergeObservable.TerminationState'):
            if self.meas_state is None:
                meas_prev_state = self.raw_prev_state.get_measured_state(self.raw_prev_termination_state)

                if isinstance(meas_prev_state, MergeObservable.NoneReceived):
                    meas_state = MergeObservable.NoneReceivedWaitAck()

                elif isinstance(meas_prev_state, MergeObservable.NoneReceivedWaitAck):
                    meas_state = MergeObservable.RightReceived(
                        elem=self.elem,
                        ack=self.ack,
                    )

                elif isinstance(meas_prev_state, MergeObservable.LeftReceived):
                    meas_state = MergeObservable.BothReceivedContinueLeft(
                            left_elem=meas_prev_state.elem,
                            right_elem=self.elem,
                            left_ack=meas_prev_state.ack,
                            right_ack=self.ack,
                        )

                elif isinstance(meas_prev_state, MergeObservable.Stopped):
                    meas_state = meas_prev_state

                else:
                    raise Exception(f'illegal state "{meas_prev_state}"')

                self.meas_state = meas_state
            else:
                meas_state = self.meas_state

            return meas_state.get_measured_state(termination_state)

    class OnAckReceived(MergeState):
        def __init__(self, ack: AckSubject):
            self.ack = ack

            self.raw_prev_termination_state: 'MergeObservable.TerminationState' = None
            self.raw_prev_state: 'MergeObservable.MergeState' = None

            self.meas_state: 'MergeObservable.MergeState' = None

        def get_measured_state(self, termination_state: 'MergeObservable.TerminationState'):
            if self.meas_state is None:
                meas_prev_state = self.raw_prev_state.get_measured_state(self.raw_prev_termination_state)

                if isinstance(meas_prev_state, MergeObservable.NoneReceivedWaitAck):
                    meas_state = MergeObservable.NoneReceived()

                elif isinstance(meas_prev_state, MergeObservable.SingleReceived):
                    meas_state = MergeObservable.NoneReceivedWaitAck()

                elif isinstance(meas_prev_state, MergeObservable.BothReceivedContinueLeft):
                    meas_state = MergeObservable.LeftReceived(
                        elem=meas_prev_state.right_elem,
                        ack=self.ack,
                    )

                elif isinstance(meas_prev_state, MergeObservable.BothReceivedContinueRight):
                    meas_state = MergeObservable.RightReceived(
                        elem=meas_prev_state.left_elem,
                        ack=self.ack,
                    )

                elif isinstance(meas_prev_state, MergeObservable.Stopped):
                    meas_state = meas_prev_state

                else:
                    raise Exception(f'illegal state "{meas_prev_state}"')

                self.meas_state = meas_state
            else:
                meas_state = self.meas_state

            return meas_state.get_measured_state(termination_state)

    class Stopped(MergeState):
        def get_measured_state(self, termination_state: 'MergeObservable.TerminationState'):
            return self

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
        self.termination_state = self.InitTerminationState()
        self.state = self.NoneReceived()

        self.lock = threading.RLock()

    class ReusltSingle(Single):
        def __init__(self, source: 'MergeObservable'):
            self.source = source

        def on_next(self, ack: AckBase):
            if isinstance(ack, Continue):
                next_state = MergeObservable.OnAckReceived(ack=AckSubject())

                with self.source.lock:
                    next_state.raw_prev_state = self.source.state
                    next_state.raw_prev_termination_state = self.source.termination_state
                    self.source.state = next_state

                meas_prev_state = next_state.raw_prev_state.get_measured_state(next_state.raw_prev_termination_state)
                meas_state = next_state.get_measured_state(next_state.raw_prev_termination_state)

                # print(meas_state)

                # acknowledgment already sent to left and right
                if isinstance(meas_state, MergeObservable.NoneReceived):
                    pass

                # send buffered element and request new one
                elif isinstance(meas_state, MergeObservable.NoneReceivedWaitAck):

                    if isinstance(meas_prev_state, MergeObservable.SingleReceived):
                        print(meas_prev_state.elem)
                        ack = self.source.observer.on_next(meas_prev_state.elem)
                        ack.on_next(meas_prev_state.ack)

                    else:
                        raise Exception(f'illegal previous state "{meas_prev_state}"')

                # send first buffered element and request new one
                elif isinstance(meas_state, MergeObservable.SingleReceived):

                    if isinstance(meas_prev_state, MergeObservable.BothReceivedContinueLeft):
                        ack = self.source.observer.on_next(meas_prev_state.left_elem)
                        ack.subscribe(self)
                        meas_prev_state.left_ack.on_next(continue_ack)

                    elif isinstance(meas_prev_state, MergeObservable.BothReceivedContinueRight):
                        ack = self.source.observer.on_next(meas_prev_state.right_elem)
                        ack.subscribe(self)
                        meas_prev_state.right_ack.on_next(continue_ack)

                    else:
                        raise Exception(f'illegal previous state "{meas_prev_state}"')

                elif isinstance(meas_state, MergeObservable.Stopped):
                    if isinstance(meas_prev_state, MergeObservable.SingleReceived):
                        self.source.observer.on_next(meas_prev_state.elem)
                        self.source.observer.on_completed()
                    else:
                        pass

                else:
                    raise Exception(f'illegal state "{meas_state}"')

        def on_error(self, exc: Exception):
            raise NotImplementedError

    def _on_next(self, next_state: 'MergeObservable.ElementReceivedBase'):
        with self.lock:
            next_state.raw_prev_state = self.state
            next_state.raw_prev_termination_state = self.termination_state
            self.state = next_state

        meas_termination_state = next_state.raw_prev_termination_state.get_measured_state()
        meas_prev_state = next_state.raw_prev_state.get_measured_state(meas_termination_state)
        meas_state = next_state.get_measured_state(meas_termination_state)

        # left is first
        if isinstance(meas_state, MergeObservable.NoneReceivedWaitAck):

            # send element
            out_ack = self.observer.on_next(next_state.elem)

            out_ack.subscribe(MergeObservable.ReusltSingle(source=self))

            return continue_ack

        elif isinstance(meas_state, MergeObservable.SingleReceived):
            pass

        elif isinstance(meas_state, MergeObservable.BothReceived):
            pass

        # keep waiting for right and acknowledment
        elif isinstance(meas_state, MergeObservable.Stopped):
            if isinstance(meas_prev_state, MergeObservable.Stopped):
                self.observer.on_completed()
                return stop_ack
            else:
                pass

        else:
            raise Exception(f'illegal state "{meas_state}"')

        return next_state.ack

    def _signal_on_complete_or_on_error(self, prev_state: 'MergeObservable.MergeState', ex: Exception = None):
        """ this function is called once

        :param raw_state:
        :param ex:
        :return:
        """

        # stop active acknowledgments
        if isinstance(prev_state, MergeObservable.NoneReceivedWaitAck):
            pass
        elif isinstance(prev_state, MergeObservable.SingleReceived):
            prev_state.ack.on_next(stop_ack)
        else:
            pass

        # terminate observer
        if ex:
            self.observer.on_error(ex)
        else:
            self.observer.on_completed()

    def _on_error_or_complete(self, next_termination_state: 'MergeObservable.TerminationState', exc: Exception = None):
        with self.lock:
            next_termination_state.raw_prev_state = self.termination_state
            self.termination_state = next_termination_state

            raw_state = self.state

        meas_prev_termination_state = next_termination_state.raw_prev_state
        meas_termination_state = next_termination_state

        meas_prev_state = raw_state.get_measured_state(meas_prev_termination_state)
        meas_state = raw_state.get_measured_state(meas_termination_state)

        # print(meas_prev_termination_state.get_measured_state())
        # print(meas_prev_state)
        # print(meas_termination_state.get_measured_state())
        # print(meas_state)

        if not isinstance(meas_prev_state, MergeObservable.Stopped) \
                and isinstance(meas_state, MergeObservable.Stopped):
            self._signal_on_complete_or_on_error(meas_prev_state, exc)

    def _on_error(self, exc: Exception):
        termination_state = self.ErrorState(exc)

        self._on_error_or_complete(next_termination_state=termination_state, exc=exc)

    def _on_completed_left(self):
        termination_state = self.LeftCompletedStateImpl()

        self._on_error_or_complete(next_termination_state=termination_state)

    def _on_completed_right(self):
        termination_state = self.RightCompletedStateImpl()

        self._on_error_or_complete(next_termination_state=termination_state)

    def observe(self, observer_info: ObserverInfo):
        self.observer = observer_info.observer
        source = self

        class LeftObserver(Observer):
            def on_next(self, elem: ElementType):
                next_state = MergeObservable.OnLeftReceived(elem=elem, ack=AckSubject())
                return source._on_next(next_state)

            def on_error(self, exc):
                source._on_error(exc)

            def on_completed(self):
                source._on_completed_left()

        class RightObserver(Observer):
            def on_next(self, elem: ElementType):
                next_state = MergeObservable.OnRightReceived(elem=elem, ack=AckSubject())
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
