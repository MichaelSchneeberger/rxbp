import threading
from typing import Callable, Generator

from rx.disposable import CompositeDisposable

from rxbp.ack import Continue, Ack
from rxbp.observable import Observable
from rxbp.observer import Observer


class MergeObservable(Observable):
    """
    :param left:
    :param right:
    :param is_lower: if right is lower than left, request next right
    :param is_higher: if right is higher than left, request next left

    Scenario 1: Send left, right arrives, out_ack returns Continue, send right, send left (preferred)
    Scenario 2: Send left, right arrives, out_ack returns Continue, send left, send right

    """

    def __init__(self, left: Observable, right: Observable):
        self.left = left
        self.right = right

    def observe(self, observer):
        class State:
            pass

        class Wait(State):
            pass

        class ElementReceived(State):
            def __init__(self, ack: Ack):
                self.ack = ack

        left_completed = [False]
        right_completed = [False]
        exception = [None]
        left_state = [Wait()]
        right_state = [Wait()]

        lock = threading.RLock()

        def on_next_left(left_elem: Callable[[], Generator]):
            # print('match left element received')

            ack = Ack()

            new_left_state = ElementReceived(ack=ack)

            with lock:
                left_state[0] = new_left_state
                meas_right_state = right_state[0]

            # left is first
            if isinstance(meas_right_state, Wait):

                # send element
                out_ack: Ack = observer.on_next(left_elem)
                out_ack.connect_ack(ack)

                def _(v):
                    if isinstance(v, Continue):

                        new_left_state = Wait()

                        with lock:
                            left_state[0] = new_left_state
                            meas_right_state = right_state[0]

                        if isinstance(meas_right_state, Wait):
                            ack.on_next(v)
                        elif isinstance(meas_right_state, ElementReceived):
                            meas_right_state.ack.connect_ack(ack)
                        else:
                            raise Exception('illegal state "{}"'.format(meas_right_state))

                out_ack.subscribe(_)

            # right was first
            elif isinstance(meas_right_state, ElementReceived):

                def _(v):
                    if isinstance(v, Continue):

                        out_ack = observer.on_next(left_elem)

                        out_ack.connect_ack(ack)

                meas_right_state.ack.subscribe(_)

            else:
                raise Exception('illegal state "{}"'.format(meas_right_state))

            return ack

        def on_next_right(right_elem: Callable[[], Generator]):
            # print('match right element received')

            ack = Ack()

            new_right_state = ElementReceived(ack=ack)

            with lock:
                right_state[0] = new_right_state
                meas_left_state = left_state[0]

            # left is first
            if isinstance(meas_left_state, Wait):

                # send element
                out_ack: Ack = observer.on_next(right_elem)
                out_ack.connect_ack(ack)

                def _(v):
                    if isinstance(v, Continue):

                        new_right_state = Wait()

                        with lock:
                            right_state[0] = new_right_state
                            meas_left_state = left_state[0]

                        if isinstance(meas_left_state, Wait):
                            ack.on_next(v)
                        elif isinstance(meas_left_state, ElementReceived):
                            meas_left_state.ack.connect_ack(ack)
                        else:
                            raise Exception('illegal state "{}"'.format(meas_left_state))

                out_ack.subscribe(_)

            # right was first
            elif isinstance(meas_left_state, ElementReceived):

                def _(v):
                    if isinstance(v, Continue):
                        out_ack = observer.on_next(right_elem)

                        out_ack.connect_ack(ack)

                meas_left_state.ack.subscribe(_)

            else:
                raise Exception('illegal state "{}"'.format(meas_left_state))

            return ack

        def on_error(exc):
            with lock:
                prev_exception = exception[0]
                exception[0] = exc

            if prev_exception is not None:
                observer.on_error(exc)

        class LeftObserver(Observer):
            def on_next(self, v):
                return on_next_left(v)

            def on_error(self, exc):
                on_error(exc)

            def on_completed(self):
                # print('left completed')

                with lock:
                    prev_left_completed = left_completed[0]
                    left_completed[0] = True
                    meas_right_completed = right_completed[0]

                if meas_right_completed and not prev_left_completed:
                    observer.on_completed()

        class RightObserver(Observer):
            def on_next(self, v):
                return on_next_right(v)

            def on_error(self, exc):
                on_error(exc)

            def on_completed(self):
                complete = False

                with lock:
                    prev_right_completed = right_completed[0]
                    right_completed[0] = True
                    meas_left_completed = left_completed[0]

                if meas_left_completed and not prev_right_completed:
                    observer.on_completed()

        left_observer = LeftObserver()
        d1 = self.left.observe(left_observer)

        right_observer = RightObserver()
        d2 = self.right.observe(right_observer)

        return CompositeDisposable(d1, d2)
