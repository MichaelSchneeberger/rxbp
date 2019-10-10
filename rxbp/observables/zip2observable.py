import itertools
import threading
from abc import ABC, abstractmethod
from typing import Callable, Any, List, Generator, Optional, Iterator

from rx.disposable import CompositeDisposable
from rxbp.ack.ackimpl import stop_ack, Continue
from rxbp.ack.ackbase import AckBase
from rxbp.ack.acksubject import AckSubject
from rxbp.observer import Observer

from rxbp.observable import Observable
from rxbp.observerinfo import ObserverInfo
from rxbp.states.measuredstates.terminationstates import TerminationStates
from rxbp.states.measuredstates.zipstates import ZipStates
from rxbp.states.rawstates.rawterminationstates import RawTerminationStates
from rxbp.states.rawstates.rawzipstates import RawZipStates
from rxbp.typing import ValueType, ElementType


class Zip2Observable(Observable):
    """ An observable that zips the elements of a left and right observable.

    Common scenario with synchronous acknowledgment (if possible):

        s1.zip(s2).subscribe(o, scheduler=s)

    ^ callstack         zip          zip
    |                   /            /
    |             o   s1       o   s1
    |            /   / ack1   /   / ack1
    |    zip   zip --       zip --
    |    /     /            /
    |   s1    s2----------- -----------     ...
    |  /     /
    | s     s                                 time
    --------------------------------------------->

    ack1: asynchronous acknowledgment returned by zip.on_next called by s1
    """

    def __init__(self, left: Observable, right: Observable, selector: Callable[[Any, Any], Any] = None):
        """
        :param left: left observable
        :param right: right observable
        :param selector: a result selector function that maps each zipped element to some result
        """

        super().__init__()

        self.left = left
        self.right = right
        self.selector = (lambda l, r: (l, r)) if selector is None else selector

        self.lock = threading.RLock()

        # Zip2Observable states
        self.observer = None
        self.termination_state = RawTerminationStates.InitState()
        self.state = RawZipStates.WaitOnLeftRight()

    def _iterate_over_batch(self, elem: ElementType, is_left: bool):

        upstream_ack = AckSubject()
        iterable = iter(elem)

        next_state = RawZipStates.ZipElements(is_left=is_left, ack=upstream_ack, iter=iterable)
        with self.lock:
            next_state.prev_raw_state = self.state
            next_state.prev_raw_termination_state = self.termination_state
            self.state = next_state

        raw_prev_termination_state = next_state.prev_raw_termination_state
        prev_raw_state = next_state.prev_raw_state
        prev_state = prev_raw_state.get_measured_state(raw_prev_termination_state)

        if isinstance(prev_state, ZipStates.Stopped):
            return stop_ack
        elif isinstance(prev_state, ZipStates.WaitOnLeftRight):
            return upstream_ack
        elif not is_left and isinstance(prev_state, ZipStates.WaitOnRight):
            left_iter = prev_state.left_iter
            right_iter = iterable
            other_upstream_ack = prev_state.left_ack
        elif is_left and isinstance(prev_state, ZipStates.WaitOnLeft):
            left_iter = iterable
            right_iter = prev_state.right_iter
            other_upstream_ack = prev_state.right_ack
        else:
            raise Exception('unknown state "{}", is_left {}'.format(prev_state, is_left))

        n1 = [None]
        def zip_gen():
            while True:
                n1[0] = None
                try:
                    n1[0] = next(left_iter)
                    n2 = next(right_iter)
                except StopIteration:
                    break

                yield self.selector(n1[0], n2)

        # buffer elements
        zipped_elements = list(zip_gen())

        # def result_gen():
        #     for e in zipped_elements:
        #         yield e

        downstream_ack = self.observer.on_next(zipped_elements)

        if n1[0] is None:
            new_left_iter = None
            do_back_pressure_left = True

            try:
                val = next(right_iter)
                new_right_iter = itertools.chain([val], right_iter)
                do_back_pressure_right = False
            except StopIteration:
                new_right_iter = None
                do_back_pressure_right = True
        else:
            new_left_iter = itertools.chain(n1, left_iter)
            new_right_iter = None

            do_back_pressure_left = False
            do_back_pressure_right = True

        # after the zip operation at least one source needs to be back-pressured
        # back-pressure both sources
        if do_back_pressure_left and do_back_pressure_right:
            next_state = RawZipStates.WaitOnLeftRight()
            downstream_ack.subscribe(upstream_ack)

        # only back-pressure right source
        elif do_back_pressure_right:

            # connect downstream with upstream ack, which is returned by this function
            if is_left:
                next_state = RawZipStates.WaitOnRight(left_iter=new_left_iter, left_ack=upstream_ack)

            # connect other upstream ack not now, but after next state is set
            else:
                next_state = RawZipStates.WaitOnRight(left_iter=new_left_iter, left_ack=other_upstream_ack)

        # only back-pressure left source
        elif do_back_pressure_left:
            if is_left:
                next_state = RawZipStates.WaitOnLeft(right_iter=new_right_iter, right_ack=other_upstream_ack)
            else:
                next_state = RawZipStates.WaitOnLeft(right_iter=new_right_iter, right_ack=upstream_ack)

        else:
            raise Exception('at least one side should be back-pressured')

        with self.lock:
            # get termination state
            raw_prev_termination_state = self.termination_state

            # set next state
            self.state = next_state

        prev_termination_state = raw_prev_termination_state.get_measured_state()

        # stop back-pressuring both sources, because there is no need to request elements
        # from completed source
        if isinstance(prev_termination_state, TerminationStates.LeftCompletedState) and do_back_pressure_left:

            # current state should be Stopped
            assert isinstance(next_state.get_measured_state(raw_prev_termination_state), ZipStates.Stopped)

            self._signal_on_complete_or_on_error(raw_state=next_state)
            other_upstream_ack.on_next(stop_ack)
            return stop_ack

        # stop back-pressuring both sources, because there is no need to request elements
        # from completed source
        elif isinstance(prev_termination_state, TerminationStates.RightCompletedState) and do_back_pressure_right:
            self._signal_on_complete_or_on_error(raw_state=next_state)
            other_upstream_ack.on_next(stop_ack)
            return stop_ack

        # in error state, stop back-pressuring both sources
        elif isinstance(prev_termination_state, TerminationStates.ErrorState):
            self._signal_on_complete_or_on_error(raw_state=next_state, exc=prev_termination_state.ex)
            other_upstream_ack.on_next(stop_ack)
            return stop_ack

        # finish connecting ack only if not in Stopped or Error state
        else:

            if do_back_pressure_left and do_back_pressure_right:
                downstream_ack.subscribe(other_upstream_ack)
                return downstream_ack

            elif do_back_pressure_right:
                if is_left:
                    downstream_ack.subscribe(other_upstream_ack)
                else:
                    # downstream_ack.subscribe(upstream_ack)
                    return downstream_ack

            elif do_back_pressure_left:
                if is_left:
                    # downstream_ack.subscribe(upstream_ack)
                    return downstream_ack
                else:
                    downstream_ack.subscribe(other_upstream_ack)

            else:
                raise Exception('at least one side should be back-pressured')

            return upstream_ack

    def _on_next_left(self, elem: ElementType):
        # try:
        return_ack = self._iterate_over_batch(elem=elem, is_left=True)
        # except Exception as exc:
        #     self.observer.on_error(exc)
        #     return stop_ack
        return return_ack

    def _on_next_right(self, elem: ElementType):
        # try:
        return_ack = self._iterate_over_batch(elem=elem, is_left=False)
        # except Exception as exc:
        #     self.observer.on_error(exc)
        #     return stop_ack
        return return_ack

    def _signal_on_complete_or_on_error(self, raw_state: ZipStates.ZipState, exc: Exception = None):
        """ this function is called once

        :param raw_state:
        :param ex:
        :return:
        """

        # stop active acknowledgments
        if isinstance(raw_state, ZipStates.WaitOnLeftRight):
            pass
        elif isinstance(raw_state, ZipStates.WaitOnLeft):
            raw_state.right_ack.on_next(stop_ack)
        elif isinstance(raw_state, ZipStates.WaitOnRight):
            raw_state.left_ack.on_next(stop_ack)
        else:
            pass

        # terminate observer
        if exc:
            self.observer.on_error(exc)
        else:
            self.observer.on_completed()

    def _on_error_or_complete(self, next_final_state: RawTerminationStates.TerminationState, exc: Exception = None):
        with self.lock:
            raw_prev_final_state = self.termination_state
            raw_prev_state = self.state
            next_final_state.raw_prev_state = raw_prev_final_state
            self.termination_state = next_final_state

        prev_state = raw_prev_state.get_measured_state(raw_prev_final_state)
        curr_state = raw_prev_state.get_measured_state(next_final_state)

        if not isinstance(prev_state, ZipStates.Stopped) \
                and isinstance(curr_state, ZipStates.Stopped):
            self._signal_on_complete_or_on_error(prev_state, exc=exc)

    def _on_error(self, exc: Exception):
        next_final_state = RawTerminationStates.ErrorState(exc=exc)

        self._on_error_or_complete(next_final_state=next_final_state, exc=exc)

    def _on_completed_left(self):
        next_final_state = RawTerminationStates.LeftCompletedState()

        self._on_error_or_complete(next_final_state=next_final_state)

    def _on_completed_right(self):
        next_final_state = RawTerminationStates.RightCompletedState()

        self._on_error_or_complete(next_final_state=next_final_state)

    def observe(self, observer_info: ObserverInfo):
        self.observer = observer_info.observer

        source = self

        class ZipLeftObserver(Observer):

            def on_next(self, elem: ElementType) -> AckBase:
                return source._on_next_left(elem)

            def on_error(self, exc: Exception):
                source._on_error(exc)

            def on_completed(self):
                source._on_completed_left()

        class ZipRightObserver(Observer):

            def on_next(self, elem: ElementType) -> AckBase:
                return source._on_next_right(elem)

            def on_error(self, exc: Exception):
                source._on_error(exc)

            def on_completed(self):
                source._on_completed_right()

        left_observer = ZipLeftObserver()
        left_subscription = observer_info.copy(left_observer)
        d1 = self.left.observe(left_subscription)

        right_observer = ZipRightObserver()
        right_subscription = observer_info.copy(right_observer)
        d2 = self.right.observe(right_subscription)

        return CompositeDisposable(d1, d2)
