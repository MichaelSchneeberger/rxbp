import itertools
from abc import ABC, abstractmethod
from typing import Callable, Any, List, Generator, Optional

from rx import config
from rx.disposables import CompositeDisposable

from rxbackpressurebatched.ack import Ack, stop_ack
from rxbackpressurebatched.observers.anonymousobserver import AnonymousObserver
from rxbackpressurebatched.observable import Observable


class Zip2Observable(Observable):
    def __init__(self, left, right, selector: Callable[[Any, Any], Any] = None):
        """ An observable that zips the elements of a left and right observable

        :param left: left observable
        :param right: right observable
        :param selector: a result selector function that maps each zipped element to some result
        """

        self.left = left
        self.right = right
        self.selector = (lambda l, r: (l, r)) if selector is None else selector     # todo: remove?

    def unsafe_subscribe(self, observer, scheduler, subscribe_scheduler):

        class FinalState(ABC):
            @abstractmethod
            def get_current_state(self):
                ...

        class InitState(FinalState):
            def get_current_state(self):
                return self

        class LeftCompletedState(FinalState):
            def __init__(self, raw_prev_state: Optional[FinalState]):
                self.raw_prev_state = raw_prev_state

            def get_current_state(self):
                prev_state = self.raw_prev_state #.get_current_state()

                if isinstance(prev_state, InitState):
                    return self
                else:
                    return self.raw_prev_state

        class RightCompletedState(FinalState):
            def __init__(self, raw_prev_state: Optional[FinalState]):
                self.raw_prev_state = raw_prev_state

            def get_current_state(self):
                prev_state = self.raw_prev_state #.get_current_state()

                if isinstance(prev_state, InitState):
                    return self
                else:
                    return self.raw_prev_state

        class ExceptionState(FinalState):
            def __init__(self, raw_prev_state: Optional[FinalState], ex: Exception):
                self.raw_prev_state = raw_prev_state
                self.ex = ex

            def get_current_state(self):
                prev_state = self.raw_prev_state #.get_current_state()

                if isinstance(prev_state, InitState):
                    return self
                else:
                    return self.raw_prev_state

        class State(ABC):
            @abstractmethod
            def get_current_state(self, final_state: FinalState):
                ...

        class WaitOnLeft(State):
            def __init__(self, left_buffer: List, right_buffer: List, right_ack: Ack,
                         right_elem: Callable[[], Generator] = None):
                self.left_buffer = left_buffer
                self.right_buffer = right_buffer
                self.right_ack = right_ack
                self.right_elem = right_elem

            def get_current_state(self, final_state: FinalState):
                if isinstance(final_state, LeftCompletedState) or isinstance(final_state, ExceptionState):
                    return Stopped()
                else:
                    return self

        class WaitOnRight(State):
            def __init__(self, left_buffer: List, right_buffer: List, left_ack: Ack,
                         left_elem: Callable[[], Generator] = None):
                self.left_buffer = left_buffer
                self.right_buffer = right_buffer
                self.left_ack = left_ack
                self.left_elem = left_elem

            def get_current_state(self, final_state: FinalState):
                if isinstance(final_state, RightCompletedState) or isinstance(final_state, ExceptionState):
                    return Stopped()
                else:
                    return self

        class WaitOnLeftRight(State):
            def __init__(self, left_buffer: List = None,
                         right_buffer: List = None):
                self.left_buffer = left_buffer
                self.right_buffer = right_buffer

            def get_current_state(self, final_state: FinalState):
                if isinstance(final_state, InitState):
                    return self
                else:
                    return Stopped()

        class ZipElements(State):
            def __init__(self, raw_prev_state: Optional[State], is_left: bool, ack: Ack, elem: Any):
                self.raw_prev_state = raw_prev_state
                self.is_left = is_left
                self.ack = ack
                self.elem = elem

            def get_current_state(self, final_state: FinalState):
                prev_state = self.raw_prev_state.get_current_state(final_state=final_state)

                if isinstance(prev_state, Stopped):
                    return prev_state
                elif isinstance(prev_state, WaitOnLeftRight):
                    if self.is_left:
                        return WaitOnRight(right_buffer=prev_state.right_buffer,
                                                left_buffer=prev_state.left_buffer,
                                                left_ack=self.ack, left_elem=self.elem) \
                            .get_current_state(final_state=final_state)
                    else:
                        return WaitOnLeft(right_buffer=prev_state.right_buffer,
                                               left_buffer=prev_state.left_buffer,
                                               right_ack=self.ack, right_elem=self.elem) \
                            .get_current_state(final_state=final_state)
                else:
                    return self

        class Stopped(State):
            def get_current_state(self, final_state: FinalState):
                return self

        lock = config['concurrency'].RLock()
        state = [WaitOnLeftRight()]
        final_state = [InitState()]

        def zip_elements(elem, is_left: bool):

            return_ack = Ack()

            next_state = ZipElements(raw_prev_state=None, is_left=is_left,
                                     ack=return_ack, elem=elem)
            with lock:
                raw_prev_state = state[0]
                raw_prev_final_state = final_state[0]
                next_state.raw_prev_state = raw_prev_state
                state[0] = next_state

            prev_final_state = raw_prev_final_state.get_current_state()
            prev_state = raw_prev_state.get_current_state(prev_final_state)

            if isinstance(prev_state, Stopped):
                return stop_ack
            elif isinstance(prev_state, WaitOnLeftRight):
                return return_ack
            elif not is_left and isinstance(prev_state, WaitOnRight):
                # received right element, start zipping
                left_buffer = prev_state.left_buffer
                right_buffer = prev_state.right_buffer
                left_elem = prev_state.left_elem
                other_ack = prev_state.left_ack
            elif is_left and isinstance(prev_state, WaitOnLeft):
                # received right element, start zipping
                left_buffer = prev_state.left_buffer
                right_buffer = prev_state.right_buffer
                right_elem = prev_state.right_elem
                other_ack = prev_state.right_ack
            else:
                raise Exception('unknown state "{}"'.format(prev_state))

            if left_buffer and is_left:
                gen1 = itertools.chain(iter(left_buffer), elem())
            elif left_buffer:
                gen1 = iter(left_buffer)
            else:
                gen1 = elem()

            if right_buffer and not is_left:
                gen2 = itertools.chain(iter(right_buffer), elem())
            elif right_buffer:
                gen2 = iter(right_buffer)
            else:
                gen2 = elem()

            def zip_gen():
                yield from zip(gen1, gen2)

            # buffer elements
            zipped_elements = list(zip_gen())

            # get rest of generator, one should be empty
            rest_left = list(gen1)
            rest_right = list(gen2)

            def result_gen():
                for e in zipped_elements:
                    yield e

            upper_ack = observer.on_next(result_gen)

            do_back_pressure_left = True
            do_back_pressure_right = True

            n_right_buffer = len(right_buffer) if right_buffer is not None else 0
            n_left_buffer = len(left_buffer) if left_buffer is not None else 0

            if rest_left:
                # are there enough elements in rest_left to not back-pressure left?
                n_right_received = len(zipped_elements) - n_right_buffer
                do_back_pressure_left = len(rest_left) < n_right_received
            elif rest_right:
                # are there enough elements in rest_right to not back-pressure left?
                # therefore, assume that the same number of elements in next left generator
                n_left_received = len(zipped_elements) - n_left_buffer

                # back-pressure when there are less elements in right buffer than left elements expected
                do_back_pressure_right = len(rest_right) < n_left_received

            if do_back_pressure_left and do_back_pressure_right:
                next_state = WaitOnLeftRight(rest_left, rest_right)
                upper_ack.connect_ack(other_ack)
                return_ack = upper_ack
            elif do_back_pressure_right:
                # only back-pressure right
                if is_left:
                    upper_ack.connect_ack(other_ack)
                    left_ack = Ack()
                    return_ack = left_ack
                else:
                    left_ack = other_ack
                    return_ack = upper_ack
                next_state = WaitOnRight(left_buffer=rest_left, right_buffer=rest_right,
                                                       left_ack=left_ack, left_elem=None)
            elif do_back_pressure_left:
                # only back-pressure left
                if is_left:
                    right_ack = other_ack
                    return_ack = upper_ack
                else:
                    right_ack = Ack()
                    upper_ack.connect_ack(other_ack)
                    return_ack = right_ack
                next_state = WaitOnLeft(left_buffer=rest_left, right_buffer=rest_right,
                                                      right_ack=right_ack, right_elem=None)
            else:
                raise Exception('at least one side should be back-pressured')

            with lock:
                raw_prev_final_state = final_state[0]
                state[0] = next_state

            prev_final_state = raw_prev_final_state.get_current_state()

            if isinstance(prev_final_state, LeftCompletedState) or isinstance(prev_final_state, RightCompletedState):
                observer.on_completed()
                return stop_ack
            elif isinstance(prev_final_state, ExceptionState):
                observer.on_error(prev_final_state.ex)
                return stop_ack
            else:
                return return_ack

        def on_next_left(elem):
            return_ack = zip_elements(elem=elem, is_left=True)
            return return_ack

        def on_next_right(elem):
            return_ack = zip_elements(elem=elem, is_left=False)
            return return_ack

        def signal_on_complete_or_on_error(raw_state, ex=None):
            # this function should not be called during transition state

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

            if ex:
                observer.on_error(ex)
            else:
                observer.on_completed()

        def on_error(ex):
            next_final_state = ExceptionState(raw_prev_state=None, ex=ex)

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

        left_observer = AnonymousObserver(on_next=on_next_left, on_error=on_error,
                                          on_completed=on_completed_left)
        d1 = self.left.unsafe_subscribe(left_observer, scheduler, subscribe_scheduler)
        right_observer = AnonymousObserver(on_next=on_next_right, on_error=on_error,
                                           on_completed=on_completed_right)
        d2 = self.right.unsafe_subscribe(right_observer, scheduler, subscribe_scheduler)

        return CompositeDisposable(d1, d2)
