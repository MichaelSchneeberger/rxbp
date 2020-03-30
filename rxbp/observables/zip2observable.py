import itertools
import threading
from typing import Callable, Any

from rx.disposable import CompositeDisposable

from rxbp.ack.acksubject import AckSubject
from rxbp.ack.mixins.ackmixin import AckMixin
from rxbp.ack.stopack import stop_ack, StopAck
from rxbp.observable import Observable
from rxbp.observer import Observer
from rxbp.observerinfo import ObserverInfo
from rxbp.states.measuredstates.terminationstates import TerminationStates
from rxbp.states.measuredstates.zipstates import ZipStates
from rxbp.states.rawstates.rawterminationstates import RawTerminationStates
from rxbp.states.rawstates.rawzipstates import RawZipStates
from rxbp.typing import ElementType


class Zip2Observable(Observable):
    """ An observable that zips the elements of a left and right observable.

    Common scenario with synchronous acknowledgment (if possible):

        s1.join_flowables(s2).subscribe(o, scheduler=s)

    ^ callstack         join_flowables          join_flowables
    |                   /            /
    |             o   s1       o   s1
    |            /   / ack1   /   / ack1
    |    join_flowables   join_flowables --       join_flowables --
    |    /     /            /
    |   s1    s2----------- -----------     ...
    |  /     /
    | s     s                                 time
    --------------------------------------------->

    ack1: asynchronous acknowledgment returned by join_flowables.on_next called by s1
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

        iterable = iter(elem)
        upstream_ack = AckSubject()

        next_state = RawZipStates.ElementReceived(
            is_left=is_left,
            ack=upstream_ack,
            iter=iterable,
        )

        with self.lock:
            next_state.prev_raw_state = self.state
            next_state.prev_raw_termination_state = self.termination_state
            self.state = next_state

        raw_prev_termination_state = next_state.prev_raw_termination_state
        curr_state = next_state.get_measured_state(raw_prev_termination_state)

        if isinstance(curr_state, ZipStates.Stopped):
            return stop_ack

        elif isinstance(curr_state, ZipStates.WaitOnRight) or isinstance(curr_state, ZipStates.WaitOnLeft):
            return upstream_ack

        elif isinstance(curr_state, ZipStates.ZipElements):
            if is_left:
                other_upstream_ack = curr_state.right_ack
            else:
                other_upstream_ack = curr_state.left_ack

        else:
            raise Exception(f'unknown state "{curr_state}", is_left {is_left}')

        # after zipping, n1 will not be None in case left
        # and right don't match in number of elements
        n1 = [None]

        def gen_zipped_elements():
            while True:
                n1[0] = None
                try:
                    n1[0] = next(curr_state.left_iter)
                    n2 = next(curr_state.right_iter)
                except StopIteration:
                    break

                yield self.selector(n1[0], n2)

        try:
            zipped_elements = list(gen_zipped_elements())
        except Exception as exc:
            self.observer.on_error(exc)
            other_upstream_ack.on_next(stop_ack)
            return stop_ack

        downstream_ack = self.observer.on_next(zipped_elements)

        if isinstance(downstream_ack, StopAck):
            other_upstream_ack.on_next(stop_ack)
            return stop_ack

        # request new element from left source
        if n1[0] is None:
            new_left_iter = None
            request_new_elem_from_left = True

            # request new element also from right source?
            try:
                val = next(curr_state.right_iter)
                new_right_iter = itertools.chain([val], curr_state.right_iter)
                request_new_elem_from_right = False

            # request new element from left and right source
            except StopIteration:
                new_right_iter = None
                request_new_elem_from_right = True

        # request new element only from right source
        else:
            new_left_iter = itertools.chain(n1, curr_state.left_iter)
            new_right_iter = None

            request_new_elem_from_left = False
            request_new_elem_from_right = True

        # define next state after zipping
        # -------------------------------

        # request new element from both sources
        if request_new_elem_from_left and request_new_elem_from_right:
            next_state = RawZipStates.WaitOnLeftRight()

        # request new element only from right source
        elif request_new_elem_from_right:

            next_state = RawZipStates.WaitOnRight(
                left_iter=new_left_iter,
                left_ack=curr_state.left_ack,
            )

        # request new element only from left source
        elif request_new_elem_from_left:

            next_state = RawZipStates.WaitOnLeft(
                right_iter=new_right_iter,
                right_ack=curr_state.right_ack,
            )

        else:
            raise Exception('after the join_flowables operation, a new element needs '
                            'to be requested from at least one source')

        with self.lock:
            # get termination state
            raw_prev_termination_state = self.termination_state

            # set next state
            self.state = next_state

        curr_state = next_state.get_measured_state(raw_prev_termination_state)

        # stop zip observable
        # previous state cannot be "Stopped", therefore don't check previous state
        if isinstance(curr_state, ZipStates.Stopped):

            prev_termination_state = raw_prev_termination_state.get_measured_state()

            if isinstance(prev_termination_state, TerminationStates.ErrorState):
                self.observer.on_error(prev_termination_state.ex)
                other_upstream_ack.on_next(stop_ack)
                return stop_ack

            else:
                self.observer.on_completed()
                other_upstream_ack.on_next(stop_ack)
                return stop_ack

        # request new elements
        else:

            if request_new_elem_from_left and request_new_elem_from_right:
                downstream_ack.subscribe(other_upstream_ack)
                return downstream_ack

            elif request_new_elem_from_right:
                if is_left:
                    downstream_ack.subscribe(other_upstream_ack)
                else:
                    return downstream_ack

            elif request_new_elem_from_left:
                if is_left:
                    return downstream_ack
                else:
                    downstream_ack.subscribe(other_upstream_ack)

            else:
                raise Exception('at least one side should be back-pressured')

            return upstream_ack

    def _on_next_left(self, elem: ElementType):
        return_ack = self._iterate_over_batch(elem=elem, is_left=True)
        return return_ack

    def _on_next_right(self, elem: ElementType):
        return_ack = self._iterate_over_batch(elem=elem, is_left=False)
        return return_ack

    def _signal_on_complete_or_on_error(
            self,
            state: ZipStates.ZipState,
            exc: Exception = None,
    ):
        """ this function is called once """

        # stop active acknowledgments
        if isinstance(state, ZipStates.WaitOnLeftRight):
            pass
        elif isinstance(state, ZipStates.WaitOnLeft):
            state.right_ack.on_next(stop_ack)
        elif isinstance(state, ZipStates.WaitOnRight):
            state.left_ack.on_next(stop_ack)
        else:
            pass

        # terminate observer
        if exc:
            self.observer.on_error(exc)
        else:
            self.observer.on_completed()

    def _on_error_or_complete(
            self,
            next_final_state: RawTerminationStates.TerminationState,
            exc: Exception = None,
    ):
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

        class ZipLeftObserver(Observer):

            def on_next(_, elem: ElementType) -> AckMixin:
                return self._on_next_left(elem)

            def on_error(_, exc: Exception):
                self._on_error(exc)

            def on_completed(_):
                self._on_completed_left()

        class ZipRightObserver(Observer):

            def on_next(_, elem: ElementType) -> AckMixin:
                return self._on_next_right(elem)

            def on_error(_, exc: Exception):
                self._on_error(exc)

            def on_completed(_):
                self._on_completed_right()

        left_observer = ZipLeftObserver()
        left_subscription = observer_info.copy(left_observer)
        d1 = self.left.observe(left_subscription)

        right_observer = ZipRightObserver()
        right_subscription = observer_info.copy(right_observer)
        d2 = self.right.observe(right_subscription)

        return CompositeDisposable(d1, d2)
