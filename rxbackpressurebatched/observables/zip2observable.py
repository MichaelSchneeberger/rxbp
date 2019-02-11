import itertools
from typing import Callable, Any, List, Generator

from rx import config
from rx.disposables import CompositeDisposable

from rxbackpressurebatched.ack import Ack, stop_ack
from rxbackpressurebatched.observers.anonymousobserver import AnonymousObserver
from rxbackpressurebatched.observable import Observable


class Zip2Observable(Observable):
    def __init__(self, left, right, selector: Callable[[Any, Any], Any] = None):
        self.left = left
        self.right = right
        self.selector = (lambda l, r: (l, r)) if selector is None else selector

    class State:
        pass

    class WaitOnLeft(State):
        def __init__(self, left_buffer: List, right_buffer: List, right_ack: Ack,
                     right_elem: Callable[[], Generator] = None):
            self.left_buffer = left_buffer
            self.right_buffer = right_buffer
            self.right_ack = right_ack
            self.right_elem = right_elem

    class WaitOnRight(State):
        def __init__(self, left_buffer: List, right_buffer: List, left_ack: Ack,
                     left_elem: Callable[[], Generator] = None):
            self.left_buffer = left_buffer
            self.right_buffer = right_buffer
            self.left_ack = left_ack
            self.left_elem = left_elem

    class WaitOnLeftRight(State):
        def __init__(self, left_buffer: List = None,
                     right_buffer: List = None):
            self.left_buffer = left_buffer
            self.right_buffer = right_buffer

    class Transition(State):
        pass

    class Completed(State):
        pass

    def unsafe_subscribe(self, observer, scheduler, subscribe_scheduler):
        lock = [config['concurrency'].RLock()]
        state = [self.WaitOnLeftRight()]
        exception = [None]
        left_completed = [False]
        right_completed = [False]

        def signal_on_complete_or_on_error(ex=None):
            # this function should not be called during transition state

            with lock[0]:
                if isinstance(state[0], self.WaitOnLeftRight):
                    new_state = self.Completed()
                elif isinstance(state[0], self.WaitOnLeft):
                    typed_state: Zip2Observable.WaitOnLeft = state[0]
                    typed_state.right_ack.on_next(stop_ack)
                    typed_state.right_ack.on_completed()
                    new_state = self.Completed()
                elif isinstance(state[0], self.WaitOnRight):
                    typed_state: Zip2Observable.WaitOnRight = state[0]
                    typed_state.left_ack.on_next(stop_ack)
                    typed_state.left_ack.on_completed()
                    new_state = self.Completed()
                else:
                    new_state = state[0]

                state[0] = new_state

            if ex:
                observer.on_error(ex)
            else:
                observer.on_completed()

        def zip_elements(elem, is_left: bool):
            left_elem: Callable = None
            right_elem: Callable = None

            with lock[0]:
                if isinstance(state[0], Zip2Observable.Completed):
                    return stop_ack
                elif isinstance(state[0], Zip2Observable.WaitOnLeftRight):
                    # received one out of two elements, wait for other one
                    typed_state: Zip2Observable.WaitOnLeftRight = state[0]

                    # create asynchronous acknowledgment
                    return_ack = Ack()

                    # change state to wait on one side
                    if is_left:
                        new_state = Zip2Observable.WaitOnRight(right_buffer=typed_state.right_buffer,
                                                               left_buffer=typed_state.left_buffer,
                                                               left_ack=return_ack, left_elem=elem)
                    else:
                        new_state = Zip2Observable.WaitOnLeft(right_buffer=typed_state.right_buffer,
                                                              left_buffer=typed_state.left_buffer,
                                                              right_ack=return_ack, right_elem=elem)
                    state[0] = new_state
                    return return_ack
                elif not is_left and isinstance(state[0], Zip2Observable.WaitOnRight):
                    # received right element, start zipping
                    typed_state: Zip2Observable.WaitOnRight = state[0]
                    left_buffer = typed_state.left_buffer
                    right_buffer = typed_state.right_buffer
                    left_elem = typed_state.left_elem
                    other_ack = typed_state.left_ack
                elif is_left and isinstance(state[0], Zip2Observable.WaitOnLeft):
                    # received right element, start zipping
                    typed_state: Zip2Observable.WaitOnLeft = state[0]
                    left_buffer = typed_state.left_buffer
                    right_buffer = typed_state.right_buffer
                    right_elem = typed_state.right_elem
                    other_ack = typed_state.right_ack
                else:
                    raise Exception('unknown state')

                # change state to transition state
                state[0] = self.Transition()

            if is_left:
                left_elem = elem
            else:
                right_elem = elem

            if left_buffer and left_elem is not None:
                gen1 = itertools.chain(iter(left_buffer), left_elem())
            elif left_buffer:
                gen1 = iter(left_buffer)
            else:
                gen1 = left_elem()

            if right_buffer and right_elem is not None:
                gen2 = itertools.chain(iter(right_buffer), right_elem())
            elif right_buffer:
                gen2 = iter(right_buffer)
            else:
                gen2 = right_elem()

            def zip_gen():
                for v1, v2 in zip(gen1, gen2):
                    yield self.selector(v1, v2)

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

            # complete if the buffer of the observable that completed is empty

            with lock[0]:
                if left_completed[0] and n_left_buffer == 0:
                    # complete observer
                    signal_on_complete_or_on_error()
                    return stop_ack
                elif right_completed[0] and n_right_buffer == 0:
                    # complete observer
                    signal_on_complete_or_on_error()
                    return stop_ack
                elif exception[0] is not None:
                    signal_on_complete_or_on_error(exception[0])
                    return stop_ack
                else:
                    if do_back_pressure_left and do_back_pressure_right:
                        new_state = Zip2Observable.WaitOnLeftRight(rest_left, rest_right)
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
                        new_state = Zip2Observable.WaitOnRight(left_buffer=rest_left, right_buffer=rest_right,
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
                        new_state = Zip2Observable.WaitOnLeft(left_buffer=rest_left, right_buffer=rest_right,
                                                              right_ack=right_ack, right_elem=None)
                    else:
                        raise Exception('at least one side should be back-pressured')

                    state[0] = new_state

            return return_ack

        def on_next_left(elem):
            # print('left')
            return_ack = zip_elements(elem=elem, is_left=True)
            return return_ack

        def on_next_right(elem):
            # print('right')
            return_ack = zip_elements(elem=elem, is_left=False)
            return return_ack

        def on_error(ex):
            forward_error = False

            with lock[0]:
                if exception[0] is not None:
                    return
                elif isinstance(state[0], self.Transition):
                    pass
                else:
                    forward_error = True
                    state[0] = Zip2Observable.Completed()

                exception[0] = ex

            if forward_error:
                signal_on_complete_or_on_error(ex)

        def on_completed_left():
            # print('complete left')
            # complete if the buffer of the observable that completed is empty
            complete = False

            with lock[0]:
                if left_completed[0]:
                    return
                elif isinstance(state[0], self.WaitOnLeftRight) or isinstance(state[0], self.WaitOnLeft):
                    left_buffer = state[0].left_buffer
                    print(left_buffer)
                    if len(left_buffer) == 0:
                        # complete
                        complete = True

                        state[0] = Zip2Observable.Completed()
                else:
                    pass

                left_completed[0] = True

            if complete:
                signal_on_complete_or_on_error()

        def on_completed_right():
            # print('complete right')
            # complete if the buffer of the observable that completed is empty
            complete = False

            with lock[0]:
                if right_completed[0]:
                    return
                elif isinstance(state[0], self.WaitOnLeftRight) or isinstance(state[0], self.WaitOnRight):
                    right_buffer = state[0].right_buffer
                    if 0 == len(right_buffer):
                        # complete
                        complete = True

                        state[0] = Zip2Observable.Completed()
                else:
                    pass

                right_completed[0] = True

            if complete:
                signal_on_complete_or_on_error()

        left_observer = AnonymousObserver(on_next=on_next_left, on_error=on_error,
                                          on_completed=on_completed_left)
        d1 = self.left.unsafe_subscribe(left_observer, scheduler, subscribe_scheduler)

        right_observer = AnonymousObserver(on_next=on_next_right, on_error=on_error,
                                           on_completed=on_completed_right)
        d2 = self.right.unsafe_subscribe(right_observer, scheduler, subscribe_scheduler)

        return CompositeDisposable(d1, d2)
