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
            def __init__(self, raw_prev_state: Optional[State], raw_prev_final_state: Optional[FinalState],
                         is_left: bool, ack: Ack, elem: Any):
                self.raw_prev_state = raw_prev_state
                self.raw_prev_final_state = raw_prev_final_state
                self.is_left = is_left
                self.ack = ack
                self.elem = elem

            def get_current_state(self, final_state: FinalState):
                # overwrite final state
                final_state = self.raw_prev_final_state.get_current_state()
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

        lock = threading.RLock()
        state = [WaitOnLeftRight()]
        final_state = [InitState()]

        def zip_elements(elem, is_left: bool):

            in_ack = Ack()

            next_state = ZipElements(raw_prev_state=None, raw_prev_final_state=None, is_left=is_left,
                                     ack=in_ack, elem=elem)
            with lock:
                raw_prev_state = state[0]
                raw_prev_final_state = final_state[0]
                next_state.raw_prev_state = raw_prev_state
                next_state.raw_prev_final_state = raw_prev_final_state
                state[0] = next_state

            prev_final_state = raw_prev_final_state.get_current_state()
            prev_state = raw_prev_state.get_current_state(prev_final_state)
            # prev_state = next_state.get_current_state(prev_final_state)

            if isinstance(prev_state, Stopped):
                return stop_ack
            elif isinstance(prev_state, WaitOnLeftRight):
                return in_ack
            elif not is_left and isinstance(prev_state, WaitOnRight):
                left_buffer = prev_state.left_buffer
                right_buffer = prev_state.right_buffer
                left_elem = prev_state.left_elem
                right_elem = elem
                other_in_ack = prev_state.left_ack
            elif is_left and isinstance(prev_state, WaitOnLeft):
                left_buffer = prev_state.left_buffer
                right_buffer = prev_state.right_buffer
                right_elem = prev_state.right_elem
                left_elem = elem
                other_in_ack = prev_state.right_ack
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
            rest_left = list(gen1) if n1[0] is None else list(gen1) + n1
            rest_right = list(gen2)

            def result_gen():
                for e in zipped_elements:
                    yield e

            upper_ack = observer.on_next(result_gen)
            # upper_ack = Ack()
            # upper_ack2.observe_on(scheduler).subscribe(upper_ack2)

            do_back_pressure_left = True
            do_back_pressure_right = True

            # print('rest left {}'.format(rest_left))
            # print('rest right {}'.format(rest_right))

            if rest_left:
                do_back_pressure_left = False
            elif rest_right:
                # print(rest_right)
                do_back_pressure_right = False
            else:
                pass

            # print('is_left ', is_left)
            # print('do_back_pressure_right ', do_back_pressure_right)
            # print('do_back_pressure_left ', do_back_pressure_left)

            if do_back_pressure_left and do_back_pressure_right:
                # upper_ack.connect_ack(other_in_ack)
                next_state = WaitOnLeftRight(rest_left, rest_right)
                in_ack = upper_ack
            elif do_back_pressure_right:
                # only back-pressure right
                if is_left:
                    left_ack = Ack()
                    in_ack = left_ack
                    # upper_ack.connect_ack(other_in_ack)
                else:
                    left_ack = other_in_ack
                    in_ack = upper_ack
                next_state = WaitOnRight(left_buffer=rest_left, right_buffer=rest_right,
                                                       left_ack=left_ack, left_elem=None)
            elif do_back_pressure_left:
                # only back-pressure left
                if is_left:
                    right_ack = other_in_ack
                    in_ack = upper_ack
                else:
                    # upper_ack.connect_ack(other_in_ack)
                    right_ack = in_ack
                next_state = WaitOnLeft(left_buffer=rest_left, right_buffer=rest_right,
                                                      right_ack=right_ack, right_elem=None)
            else:
                raise Exception('at least one side should be back-pressured')

            with lock:
                raw_prev_final_state = final_state[0]
                state[0] = next_state

            prev_final_state = raw_prev_final_state.get_current_state()

            if isinstance(prev_final_state, LeftCompletedState) and do_back_pressure_left:
                signal_on_complete_or_on_error(raw_state=next_state)
                other_in_ack.on_next(stop_ack)
                other_in_ack.on_completed()
                return stop_ack
            elif isinstance(prev_final_state, RightCompletedState) and do_back_pressure_right:
                signal_on_complete_or_on_error(raw_state=next_state)
                other_in_ack.on_next(stop_ack)
                other_in_ack.on_completed()
                return stop_ack
            elif isinstance(prev_final_state, ExceptionState):
                signal_on_complete_or_on_error(raw_state=next_state, ex=prev_final_state.ex)
                other_in_ack.on_next(stop_ack)
                other_in_ack.on_completed()
                return stop_ack
            else:
                if do_back_pressure_left and do_back_pressure_right:
                    upper_ack.connect_ack(other_in_ack)
                elif do_back_pressure_right:
                    # only back-pressure right
                    if is_left:
                        upper_ack.connect_ack(other_in_ack)
                elif do_back_pressure_left:
                    # only back-pressure left
                    if not is_left:
                        upper_ack.connect_ack(other_in_ack)
                else:
                    raise Exception('at least one side should be back-pressured')

                return in_ack

        def on_next_left(elem):
            # print('zip on_next_left')
            try:
                return_ack = zip_elements(elem=elem, is_left=True)
                # return_ack.subscribe(print)
            except:
                import traceback
                traceback.print_exc()
                raise
            return return_ack

        def on_next_right(elem):
            # print('zip on_next_right')
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
            # print('zip on_completed_left')
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
            # print('zip on_completed_right')
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
