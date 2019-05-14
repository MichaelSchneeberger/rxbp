import itertools
import threading
from abc import ABC, abstractmethod
from typing import Callable, Any, List, Generator, Optional

from rx.disposable import CompositeDisposable

from rxbp.ack import Ack, stop_ack
from rxbp.observers.anonymousobserver import AnonymousObserver
from rxbp.observable import Observable


class Zip2Observable(Observable):
    def __init__(self, left: Observable, right: Observable, selector: Callable[[Any, Any], Any] = None):
        """ An observable that zips the elements of a left and right observable

        :param left: left observable
        :param right: right observable
        :param selector: a result selector function that maps each zipped element to some result
        """

        super().__init__()

        self.left = left
        self.right = right
        self.selector = (lambda l, r: (l, r)) if selector is None else selector

    def observe(self, observer):

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
            def __init__(self, raw_prev_state: Optional[TerminationState]):
                self.raw_prev_state = raw_prev_state

            def get_current_state(self):
                prev_state = self.raw_prev_state

                if isinstance(prev_state, InitState):
                    return self
                else:
                    return self.raw_prev_state

        class RightCompletedState(TerminationState):
            def __init__(self, raw_prev_state: Optional[TerminationState]):
                self.raw_prev_state = raw_prev_state

            def get_current_state(self):
                prev_state = self.raw_prev_state

                if isinstance(prev_state, InitState):
                    return self
                else:
                    return self.raw_prev_state

        class ErrorState(TerminationState):
            def __init__(self, raw_prev_state: Optional[TerminationState], ex: Exception):
                self.raw_prev_state = raw_prev_state
                self.ex = ex

            def get_current_state(self):
                prev_state = self.raw_prev_state

                if isinstance(prev_state, InitState):
                    return self
                else:
                    return self.raw_prev_state

        class ZipState(ABC):
            """ The state of the zip observable actor

            The true state (at the point when it is read) is only known by calling  the
            `get_current_state` method.
            """

            @abstractmethod
            def get_current_state(self, final_state: TerminationState):
                ...

        class WaitOnLeft(ZipState):
            """ Zip observable actor has or will back-pressure the left source, but no element
            has yet been received.

            In this state, the left buffer is empty.
            """

            def __init__(self, right_ack: Ack, right_buffer: List = None,
                         right_elem: Callable[[], Generator] = None):
                self.right_buffer = right_buffer
                self.right_ack = right_ack
                self.right_elem = right_elem

            def get_current_state(self, final_state: TerminationState):
                if isinstance(final_state, LeftCompletedState) or isinstance(final_state, ErrorState):
                    return Stopped()
                else:
                    return self

        class WaitOnRight(ZipState):
            """ Equivalent of WaitOnLeft """

            def __init__(self, left_ack: Ack, left_buffer: List = None,
                         left_elem: Callable[[], Generator] = None):
                self.left_buffer = left_buffer
                self.left_ack = left_ack
                self.left_elem = left_elem

            def get_current_state(self, final_state: TerminationState):
                if isinstance(final_state, RightCompletedState) or isinstance(final_state, ErrorState):
                    return Stopped()
                else:
                    return self

        class WaitOnLeftRight(ZipState):
            """ Zip observable actor has or will back-pressure the left and right source, but
            no element has yet been received.

            In this state, the left and right buffer are empty.
            """

            def get_current_state(self, final_state: TerminationState):
                if isinstance(final_state, InitState):
                    return self
                else:
                    return Stopped()

        class ZipElements(ZipState):
            """ Zip observable actor is zipping the values just received by a source and
             from the buffer.

            In this state the actual termination state is ignored in the `get_current_state`
            method.
            """

            def __init__(self, is_left: bool, ack: Ack, elem: Callable[[], Generator]):
                """
                :param is_left:
                :param ack:
                :param elem:
                """

                self.is_left = is_left
                self.ack = ack
                self.elem = elem

                # to be overwritten synchronously right after initializing the object
                self.raw_prev_state = None
                self.raw_prev_terminal_state = None

            def get_current_state(self, final_state: TerminationState):
                # overwrite final state
                final_state_ = self.raw_prev_terminal_state.get_current_state()
                prev_state = self.raw_prev_state.get_current_state(final_state=final_state_)

                if isinstance(prev_state, Stopped):
                    return prev_state

                # Needed for `signal_on_complete_or_on_error`
                elif isinstance(prev_state, WaitOnLeftRight):
                    if self.is_left:
                        return WaitOnRight(left_ack=self.ack, left_elem=self.elem) \
                            .get_current_state(final_state=final_state_)
                    else:
                        return WaitOnLeft(right_ack=self.ack, right_elem=self.elem) \
                            .get_current_state(final_state=final_state_)

                else:
                    return self

        class Stopped(ZipState):
            def get_current_state(self, final_state: TerminationState):
                return self

        lock = threading.RLock()
        state = [WaitOnLeftRight()]
        final_state = [InitState()]

        def zip_elements(elem: Callable[[], Generator], is_left: bool):

            #
            upstream_ack = Ack()

            next_state = ZipElements(is_left=is_left,
                                     ack=upstream_ack, elem=elem)
            with lock:
                raw_prev_state = state[0]
                raw_prev_termination_state = final_state[0]
                next_state.raw_prev_state = raw_prev_state
                next_state.raw_prev_terminal_state = raw_prev_termination_state
                state[0] = next_state

            prev_termination_state = raw_prev_termination_state.get_current_state()
            prev_state = raw_prev_state.get_current_state(prev_termination_state)

            if isinstance(prev_state, Stopped):
                return stop_ack
            elif isinstance(prev_state, WaitOnLeftRight):
                return upstream_ack
            elif not is_left and isinstance(prev_state, WaitOnRight):
                left_buffer = prev_state.left_buffer
                right_buffer = None
                left_elem = prev_state.left_elem
                right_elem = elem
                other_upstream_ack = prev_state.left_ack
            elif is_left and isinstance(prev_state, WaitOnLeft):
                left_buffer = None
                right_buffer = prev_state.right_buffer
                right_elem = prev_state.right_elem
                left_elem = elem
                other_upstream_ack = prev_state.right_ack
            else:
                raise Exception('unknown state "{}", is_left {}'.format(prev_state, is_left))

            if left_elem is not None:
                if left_buffer:
                    gen1 = itertools.chain(iter(left_buffer), left_elem())
                else:
                    gen1 = left_elem()
            elif left_buffer:
                gen1 = iter(left_buffer)
            else:
                raise Exception('illegal state')

            if right_elem is not None:
                if right_buffer:
                    gen2 = itertools.chain(iter(right_buffer), right_elem())
                else:
                    gen2 = right_elem()
            elif right_buffer:
                gen2 = iter(right_buffer)
            else:
                raise Exception('illegal state')

            n1 = [None]
            def zip_gen():
                while True:
                    n1[0] = None
                    try:
                        n1[0] = next(gen1)
                        n2 = next(gen2)
                    except StopIteration:
                        break

                    yield self.selector(n1[0], n2)

            # buffer elements
            zipped_elements = list(zip_gen())

            # get rest of generator, one should be empty
            rest_left = list(gen1) if n1[0] is None else n1 + list(gen1)
            rest_right = list(gen2)

            def result_gen():
                for e in zipped_elements:
                    yield e

            downstream_ack = observer.on_next(result_gen)

            do_back_pressure_left = True
            do_back_pressure_right = True

            # back-pressure one side only if the equivalent buffer is empty
            if rest_left:
                do_back_pressure_left = False
            elif rest_right:
                do_back_pressure_right = False
            else:
                pass

            # after the zip operation at least one source needs to be back-pressured
            # back-pressure both sources
            if do_back_pressure_left and do_back_pressure_right:
                next_state = WaitOnLeftRight()
                downstream_ack.connect_ack(upstream_ack)

            # only back-pressure right source
            elif do_back_pressure_right:

                # connect downstream with upstream ack, which is returned by this function
                if not is_left:
                    downstream_ack.connect_ack(upstream_ack)

                    next_state = WaitOnRight(left_buffer=rest_left,
                                             left_ack=other_upstream_ack, left_elem=None)

                # connect other upstream ack not now, but after next state is set
                else:
                    next_state = WaitOnRight(left_buffer=rest_left,
                                             left_ack=upstream_ack, left_elem=None)

            # only back-pressure left source
            elif do_back_pressure_left:
                if is_left:
                    downstream_ack.connect_ack(upstream_ack)

                    next_state = WaitOnLeft(right_buffer=rest_right,
                                            right_ack=other_upstream_ack, right_elem=None)
                else:
                    next_state = WaitOnLeft(right_buffer=rest_right,
                                            right_ack=upstream_ack, right_elem=None)

            else:
                raise Exception('at least one side should be back-pressured')

            with lock:
                # get termination state
                raw_prev_termination_state = final_state[0]

                # set next state
                state[0] = next_state

            prev_termination_state = raw_prev_termination_state.get_current_state()

            # stop back-pressuring both sources, because there is no need to request elements
            # from completed source
            if isinstance(prev_termination_state, LeftCompletedState) and do_back_pressure_left:

                # current state should be Stopped
                assert isinstance(next_state.get_current_state(prev_termination_state), Stopped)

                signal_on_complete_or_on_error(raw_state=next_state)
                other_upstream_ack.on_next(stop_ack)
                other_upstream_ack.on_completed()
                return stop_ack

            # stop back-pressuring both sources, because there is no need to request elements
            # from completed source
            elif isinstance(prev_termination_state, RightCompletedState) and do_back_pressure_right:
                signal_on_complete_or_on_error(raw_state=next_state)
                other_upstream_ack.on_next(stop_ack)
                other_upstream_ack.on_completed()
                return stop_ack

            # in error state, stop back-pressuring both sources
            elif isinstance(prev_termination_state, ErrorState):
                signal_on_complete_or_on_error(raw_state=next_state, ex=prev_termination_state.ex)
                other_upstream_ack.on_next(stop_ack)
                other_upstream_ack.on_completed()
                return stop_ack

            # finish connecting ack
            else:

                if do_back_pressure_left and do_back_pressure_right:
                    downstream_ack.connect_ack(other_upstream_ack)

                elif do_back_pressure_right:
                    if is_left:
                        downstream_ack.connect_ack(other_upstream_ack)

                elif do_back_pressure_left:
                    if not is_left:
                        downstream_ack.connect_ack(other_upstream_ack)

                else:
                    raise Exception('at least one side should be back-pressured')

                return upstream_ack

        def on_next_left(elem):
            try:
                return_ack = zip_elements(elem=elem, is_left=True)
            except Exception as exc:
                observer.on_error(exc)
                return stop_ack
            return return_ack

        def on_next_right(elem):
            try:
                return_ack = zip_elements(elem=elem, is_left=False)
            except Exception as exc:
                observer.on_error(exc)
                return stop_ack
            return return_ack

        def signal_on_complete_or_on_error(raw_state: ZipState, ex: Exception = None):
            """ this function is called once

            :param raw_state:
            :param ex:
            :return:
            """

            # stop active acknowledgments
            if isinstance(raw_state, WaitOnLeftRight):
                pass
            elif isinstance(raw_state, WaitOnLeft):
                raw_state.right_ack.on_next(stop_ack)
                raw_state.right_ack.on_completed()
            elif isinstance(raw_state, WaitOnRight):
                raw_state.left_ack.on_next(stop_ack)
                raw_state.left_ack.on_completed()
            else:
                pass

            # terminate observer
            if ex:
                observer.on_error(ex)
            else:
                observer.on_completed()

        def on_error(ex):
            next_final_state = ErrorState(raw_prev_state=None, ex=ex)

            with lock:
                raw_prev_final_state = final_state[0]
                raw_prev_state = state[0]
                next_final_state.raw_prev_state = raw_prev_final_state
                final_state[0] = next_final_state

            prev_final_state = raw_prev_final_state.get_current_state()
            prev_state = raw_prev_state.get_current_state(final_state=prev_final_state)

            curr_final_state = next_final_state.get_current_state()
            curr_state = raw_prev_state.get_current_state(final_state=curr_final_state)

            if not isinstance(prev_state, Stopped) and isinstance(curr_state, Stopped):
                signal_on_complete_or_on_error(raw_state=raw_prev_state, ex=ex)

        def on_completed_left():
            next_final_state = LeftCompletedState(raw_prev_state=None)

            with lock:
                raw_prev_final_state = final_state[0]
                raw_prev_state = state[0]
                next_final_state.raw_prev_state = raw_prev_final_state
                final_state[0] = next_final_state

            prev_final_state = raw_prev_final_state.get_current_state()
            prev_state = raw_prev_state.get_current_state(final_state=prev_final_state)

            curr_final_state = next_final_state.get_current_state()
            curr_state = raw_prev_state.get_current_state(final_state=curr_final_state)

            if not isinstance(prev_state, Stopped) and isinstance(curr_state, Stopped):
                signal_on_complete_or_on_error(raw_state=raw_prev_state)

        def on_completed_right():
            next_final_state = RightCompletedState(raw_prev_state=None)

            with lock:
                raw_prev_final_state = final_state[0]
                raw_prev_state = state[0]
                next_final_state.raw_prev_state = raw_prev_final_state
                final_state[0] = next_final_state

            prev_final_state = raw_prev_final_state.get_current_state()
            prev_state = raw_prev_state.get_current_state(final_state=prev_final_state)

            curr_final_state = next_final_state.get_current_state()
            curr_state = raw_prev_state.get_current_state(final_state=curr_final_state)

            if not isinstance(prev_state, Stopped) and isinstance(curr_state, Stopped):
                signal_on_complete_or_on_error(raw_state=raw_prev_state)

        left_observer = AnonymousObserver(on_next_func=on_next_left, on_error_func=on_error,
                                          on_completed_func=on_completed_left)
        d1 = self.left.observe(left_observer)
        right_observer = AnonymousObserver(on_next_func=on_next_right, on_error_func=on_error,
                                           on_completed_func=on_completed_right)
        d2 = self.right.observe(right_observer)

        return CompositeDisposable(d1, d2)
