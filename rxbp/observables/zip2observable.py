import itertools
import threading
from abc import ABC, abstractmethod
from typing import Callable, Any, List, Generator, Optional, Iterator

from rx.disposable import CompositeDisposable
from rxbp.ack.ackimpl import stop_ack
from rxbp.ack.ackbase import AckBase
from rxbp.ack.acksubject import AckSubject

from rxbp.observers.anonymousobserver import AnonymousObserver
from rxbp.observable import Observable
from rxbp.observerinfo import ObserverInfo


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
        self.termination_state = Zip2Observable.InitState()
        self.zip_state = Zip2Observable.WaitOnLeftRight()

    class TerminationState(ABC):
        """ The zip observable actor state is best captured with two states, to avoid having to
        enumerate a big number of states. The termination state is changed on the `on_complete`
        or `on_error` method call.

        The ZipState depends on the TerminationState but not vice versa.
        """

        @abstractmethod
        def get_current_state(self):
            ...

    class InitState(TerminationState):
        def get_current_state(self):
            return self

    class LeftCompletedState(TerminationState):
        def __init__(self, raw_prev_state: Optional['Zip2Observable.TerminationState']):
            self.raw_prev_state = raw_prev_state

        def get_current_state(self):
            prev_state = self.raw_prev_state

            if isinstance(prev_state, Zip2Observable.InitState):
                return self
            else:
                return self.raw_prev_state

    class RightCompletedState(TerminationState):
        def __init__(self, raw_prev_state: Optional['Zip2Observable.TerminationState']):
            self.raw_prev_state = raw_prev_state

        def get_current_state(self):
            prev_state = self.raw_prev_state

            if isinstance(prev_state, Zip2Observable.InitState):
                return self
            else:
                return self.raw_prev_state

    class ErrorState(TerminationState):
        def __init__(self, raw_prev_state: Optional['Zip2Observable.TerminationState'], ex: Exception):
            self.raw_prev_state = raw_prev_state
            self.ex = ex

        def get_current_state(self):
            prev_state = self.raw_prev_state

            return self
            # if isinstance(prev_state, Zip2Observable.InitState):
            #     return self
            # else:
            #     return self.raw_prev_state

    class ZipState(ABC):
        """ The state of the zip observable actor

        The true state (at the point when it is read) is only known by calling  the
        `get_current_state` method.
        """

        @abstractmethod
        def get_current_state(self, final_state: 'Zip2Observable.TerminationState'):
            ...

    class WaitOnLeft(ZipState):
        """ Zip observable actor has or will back-pressure the left source, but no element
        has yet been received.

        In this state, the left buffer is empty.
        """

        def __init__(self, right_ack: AckBase, right_iter: Iterator):
            self.right_ack = right_ack
            self.right_iter = right_iter

        def get_current_state(self, final_state: 'Zip2Observable.TerminationState'):
            if isinstance(final_state, Zip2Observable.LeftCompletedState) or isinstance(final_state, Zip2Observable.ErrorState):
                return Zip2Observable.Stopped()
            else:
                return self

    class WaitOnRight(ZipState):
        """ Equivalent of WaitOnLeft """

        def __init__(self, left_ack: AckBase, left_iter: Iterator):
            self.left_iter = left_iter
            self.left_ack = left_ack

        def get_current_state(self, final_state: 'Zip2Observable.TerminationState'):
            if isinstance(final_state, Zip2Observable.RightCompletedState) or isinstance(final_state, Zip2Observable.ErrorState):
                return Zip2Observable.Stopped()
            else:
                return self

    class WaitOnLeftRight(ZipState):
        """ Zip observable actor has or will back-pressure the left and right source, but
        no element has yet been received.

        In this state, the left and right buffer are empty.
        """

        def get_current_state(self, final_state: 'Zip2Observable.TerminationState'):
            if isinstance(final_state, Zip2Observable.InitState):
                return self
            else:
                return Zip2Observable.Stopped()

    class ZipElements(ZipState):
        """ Zip observable actor is zipping the values just received by a source and
         from the buffer.

        In this state the actual termination state is ignored in the `get_current_state`
        method.
        """

        def __init__(self, is_left: bool, ack: AckBase, iter: Iterator):
            """
            :param is_left:
            :param ack:
            :param elem:
            """

            self.is_left = is_left
            self.ack = ack
            self.iter = iter

            # to be overwritten synchronously right after initializing the object
            self.raw_prev_state = None
            self.raw_prev_terminal_state = None

        def get_current_state(self, final_state: 'Zip2Observable.TerminationState'):
            # overwrite final state
            final_state_ = self.raw_prev_terminal_state.get_current_state()
            prev_state = self.raw_prev_state.get_current_state(final_state=final_state_)

            if isinstance(prev_state, Zip2Observable.Stopped):
                return prev_state

            # Needed for `signal_on_complete_or_on_error`
            elif isinstance(prev_state, Zip2Observable.WaitOnLeftRight):
                if self.is_left:
                    return Zip2Observable.WaitOnRight(left_ack=self.ack, left_iter=self.iter) \
                        .get_current_state(final_state=final_state)
                else:
                    return Zip2Observable.WaitOnLeft(right_ack=self.ack, right_iter=self.iter) \
                        .get_current_state(final_state=final_state)

            else:
                return self

    class Stopped(ZipState):
        def get_current_state(self, final_state: 'Zip2Observable.TerminationState'):
            return self

    def _iterate_over_batch(self, elem: Callable[[], Generator], is_left: bool):

        upstream_ack = AckSubject()
        iter = elem()

        next_state = Zip2Observable.ZipElements(is_left=is_left, ack=upstream_ack, iter=iter)
        with self.lock:
            raw_prev_state = self.zip_state
            raw_prev_termination_state = self.termination_state
            next_state.raw_prev_state = raw_prev_state
            next_state.raw_prev_terminal_state = raw_prev_termination_state
            self.zip_state = next_state

        prev_termination_state = raw_prev_termination_state.get_current_state()
        prev_state = raw_prev_state.get_current_state(prev_termination_state)

        if isinstance(prev_state, Zip2Observable.Stopped):
            return stop_ack
        elif isinstance(prev_state, Zip2Observable.WaitOnLeftRight):
            return upstream_ack
        elif not is_left and isinstance(prev_state, Zip2Observable.WaitOnRight):
            left_iter = prev_state.left_iter
            right_iter = iter
            other_upstream_ack = prev_state.left_ack
        elif is_left and isinstance(prev_state, Zip2Observable.WaitOnLeft):
            left_iter = iter
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

        def result_gen():
            for e in zipped_elements:
                yield e

        downstream_ack = self.observer.on_next(result_gen)

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
            next_state = Zip2Observable.WaitOnLeftRight()
            downstream_ack.subscribe(upstream_ack)

        # only back-pressure right source
        elif do_back_pressure_right:

            # connect downstream with upstream ack, which is returned by this function
            if is_left:
                next_state = Zip2Observable.WaitOnRight(left_iter=new_left_iter, left_ack=upstream_ack)

            # connect other upstream ack not now, but after next state is set
            else:
                next_state = Zip2Observable.WaitOnRight(left_iter=new_left_iter, left_ack=other_upstream_ack)

        # only back-pressure left source
        elif do_back_pressure_left:
            if is_left:
                next_state = Zip2Observable.WaitOnLeft(right_iter=new_right_iter, right_ack=other_upstream_ack)
            else:
                next_state = Zip2Observable.WaitOnLeft(right_iter=new_right_iter, right_ack=upstream_ack)

        else:
            raise Exception('at least one side should be back-pressured')

        with self.lock:
            # get termination state
            raw_prev_termination_state = self.termination_state

            # set next state
            self.zip_state = next_state

        prev_termination_state = raw_prev_termination_state.get_current_state()

        # stop back-pressuring both sources, because there is no need to request elements
        # from completed source
        if isinstance(prev_termination_state, Zip2Observable.LeftCompletedState) and do_back_pressure_left:

            # current state should be Stopped
            assert isinstance(next_state.get_current_state(prev_termination_state), Zip2Observable.Stopped)

            self._signal_on_complete_or_on_error(raw_state=next_state)
            other_upstream_ack.on_next(stop_ack)
            return stop_ack

        # stop back-pressuring both sources, because there is no need to request elements
        # from completed source
        elif isinstance(prev_termination_state, Zip2Observable.RightCompletedState) and do_back_pressure_right:
            self._signal_on_complete_or_on_error(raw_state=next_state)
            other_upstream_ack.on_next(stop_ack)
            return stop_ack

        # in error state, stop back-pressuring both sources
        elif isinstance(prev_termination_state, Zip2Observable.ErrorState):
            self._signal_on_complete_or_on_error(raw_state=next_state, ex=prev_termination_state.ex)
            other_upstream_ack.on_next(stop_ack)
            return stop_ack

        # finish connecting ack only if not in Stopped or Error state
        else:

            if do_back_pressure_left and do_back_pressure_right:
                downstream_ack.subscribe(other_upstream_ack)

            elif do_back_pressure_right:
                if is_left:
                    downstream_ack.subscribe(other_upstream_ack)
                else:
                    downstream_ack.subscribe(upstream_ack)

            elif do_back_pressure_left:
                if is_left:
                    downstream_ack.subscribe(upstream_ack)
                else:
                    downstream_ack.subscribe(other_upstream_ack)

            else:
                raise Exception('at least one side should be back-pressured')

            return upstream_ack

    def _on_next_left(self, elem):
        try:
            return_ack = self._iterate_over_batch(elem=elem, is_left=True)
        except Exception as exc:
            self.observer.on_error(exc)
            return stop_ack
        return return_ack

    def _on_next_right(self, elem):
        try:
            return_ack = self._iterate_over_batch(elem=elem, is_left=False)
        except Exception as exc:
            self.observer.on_error(exc)
            return stop_ack
        return return_ack

    def _signal_on_complete_or_on_error(self, raw_state: 'Zip2Observable.ZipState', ex: Exception = None):
        """ this function is called once

        :param raw_state:
        :param ex:
        :return:
        """

        # stop active acknowledgments
        if isinstance(raw_state, Zip2Observable.WaitOnLeftRight):
            pass
        elif isinstance(raw_state, Zip2Observable.WaitOnLeft):
            raw_state.right_ack.on_next(stop_ack)
        elif isinstance(raw_state, Zip2Observable.WaitOnRight):
            raw_state.left_ack.on_next(stop_ack)
        else:
            pass

        # terminate observer
        if ex:
            self.observer.on_error(ex)
        else:
            self.observer.on_completed()

    def _on_error_or_complete(self, next_final_state: 'Zip2Observable.TerminationState', exc: Exception = None):
        with self.lock:
            raw_prev_final_state = self.termination_state
            raw_prev_state = self.zip_state
            next_final_state.raw_prev_state = raw_prev_final_state
            self.termination_state = next_final_state

        prev_final_state = raw_prev_final_state.get_current_state()
        prev_state = raw_prev_state.get_current_state(final_state=prev_final_state)

        curr_final_state = next_final_state.get_current_state()
        curr_state = raw_prev_state.get_current_state(final_state=curr_final_state)

        if not isinstance(prev_state, Zip2Observable.Stopped) \
                and isinstance(curr_state, Zip2Observable.Stopped):
            self._signal_on_complete_or_on_error(raw_prev_state, exc)

    def _on_error(self, ex):
        next_final_state = Zip2Observable.ErrorState(raw_prev_state=None, ex=ex)

        self._on_error_or_complete(next_final_state=next_final_state, exc=ex)

    def _on_completed_left(self):
        next_final_state = Zip2Observable.LeftCompletedState(raw_prev_state=None)

        self._on_error_or_complete(next_final_state=next_final_state)

    def _on_completed_right(self):
        next_final_state = Zip2Observable.RightCompletedState(raw_prev_state=None)

        self._on_error_or_complete(next_final_state=next_final_state)

    def observe(self, observer_info: ObserverInfo):
        self.observer = observer_info.observer

        left_observer = AnonymousObserver(on_next_func=self._on_next_left, on_error_func=self._on_error,
                                          on_completed_func=self._on_completed_left)
        left_subscription = observer_info.copy(left_observer)
        d1 = self.left.observe(left_subscription)

        right_observer = AnonymousObserver(on_next_func=self._on_next_right, on_error_func=self._on_error,
                                           on_completed_func=self._on_completed_right)
        right_subscription = observer_info.copy(right_observer)
        d2 = self.right.observe(right_subscription)

        return CompositeDisposable(d1, d2)
