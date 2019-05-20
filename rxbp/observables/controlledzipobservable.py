import threading
from typing import Callable, Any, Generator, Iterator, Tuple, Optional

from rx.disposable import CompositeDisposable

from rxbp.ack import Ack, continue_ack, stop_ack
from rxbp.observers.anonymousobserver import AnonymousObserver
from rxbp.selectors.selection import select_next, select_completed
from rxbp.observable import Observable
from rxbp.observer import Observer
from rxbp.scheduler import Scheduler
from rxbp.subjects.publishsubject import PublishSubject


class ControlledZipObservable(Observable):
    def __init__(self, left: Observable, right: Observable,
                 request_left: Callable[[Any, Any], bool],
                 request_right: Callable[[Any, Any], bool],
                 match_func: Callable[[Any, Any], bool],
                 scheduler: Scheduler):
        """
        :param left: lef observable
        :param right: right observable
        :param is_lower: if right is lower than left, request next right
        :param is_higher: if right is higher than left, request next left
        """

        # save input arguments
        self.left_observable = left
        self.right_observable = right
        self.request_left = request_left
        self.request_right = request_right
        self.match_func = match_func

        # create two selector subjects used to match Flowables
        self.left_selector = PublishSubject(scheduler=scheduler)
        self.right_selector = PublishSubject(scheduler=scheduler)

        self.lock = threading.RLock()

        # ControlledZipObservable state
        self.observer = None
        self.exception = None
        self.left_completed = False
        self.right_completed = False
        self.state = ControlledZipObservable.WaitLeftRight()

    class State:
        pass

    class WaitLeftRight(State):
        pass

    class WaitOnLeft(State):
        def __init__(self,
                     right_val: Any,
                     right_iter: Iterator,
                     right_in_ack: Ack,
                     right_out_ack: Optional[Ack]
                     ):
            self.right_val = right_val
            self.right_iter = right_iter
            self.right_in_ack = right_in_ack
            self.right_out_ack = right_out_ack

    class WaitOnRight(State):
        def __init__(self,
                     left_iter: Iterator,
                     left_val: Any,
                     left_in_ack: Ack,
                     left_out_ack: Optional[Ack]):
            self.left_val = left_val
            self.left_iter = left_iter
            self.left_in_ack = left_in_ack
            self.left_out_ack = left_out_ack

    class Transition(State):
        pass

    class Completed(State):
        pass

    class ConcurrentType:
        pass

    class SynchronousLeft(ConcurrentType):
        """ Object indicating if function is called synchronously
        """

        def __init__(self, right_in_ack: Ack):
            self.right_in_ack = right_in_ack

    class SynchronousRight(ConcurrentType):
        """ Object indicating if function is called synchronously
        """

        def __init__(self, left_in_ack: Ack):
            self.left_in_ack = left_in_ack

    def iterate_over_batch(self, left_val: Any,
                           last_left_out_ack: Optional[Ack], left_iter: Iterator[Tuple[Any, PublishSubject]],
                           right_val: Optional[Any], right_iter: Iterator[Any], last_right_out_ack: Optional[Ack],
                           is_sync: 'ControlledZipObservable.ConcurrentType') \
            -> Ack:

        # keep elements to be sent in a buffer. Only when the incoming batch of elements is iterated over, the
        # elements in the buffer are sent.
        left_index_buffer = []                  # index of the elements from the left observable that got selected
        right_index_buffer = []                 #   by the match function
        zipped_output_buffer = []

        has_left_elem = True
        has_right_elem = True

        # iterate over two batches of elements from left and right observable until one batch is empty
        while True:

            # print('left_val={}, right_val={}'.format(left_val, right_val))

            if self.match_func(left_val, right_val):
                left_index_buffer.append(select_next)
                right_index_buffer.append(select_next)

                # add to buffer
                zipped_output_buffer.append((left_val, right_val))

            # save old value before left_val is overwritten
            old_left_val = left_val

            if self.request_left(left_val, right_val):
                left_index_buffer.append(select_completed)
                try:
                    left_val = next(left_iter)
                except StopIteration:
                    has_left_elem = False

            if self.request_right(old_left_val, right_val):
                right_index_buffer.append(select_completed)

                try:
                    right_val = next(right_iter)
                except StopIteration:
                    has_right_elem = False
                    break

            if not has_left_elem:
                break

        # only send elements downstream, if there are any to be sent
        if zipped_output_buffer:
            # create a generate function to be sent
            def gen():
                yield from zipped_output_buffer

            zip_out_ack = self.observer.on_next(gen)
        else:
            zip_out_ack = continue_ack

        # only send elements over the left selector observer, if there are any to be sent
        if left_index_buffer:
            def gen():
                yield from left_index_buffer

            left_out_ack = self.left_selector.on_next(gen)
        else:
            left_out_ack = last_left_out_ack or continue_ack

        # only send elements over the right selector observer, if there are any to be sent
        if right_index_buffer:
            def gen():
                yield from right_index_buffer

            right_out_ack = self.right_selector.on_next(gen)
        else:
            right_out_ack = last_right_out_ack or continue_ack

        # all elements in the left and right buffer are send to the observer
        if not has_left_elem and not has_right_elem:
            next_state = ControlledZipObservable.WaitLeftRight()
            self.state = next_state

            result_ack_left = zip_out_ack.merge_ack(left_out_ack)
            result_ack_right = zip_out_ack.merge_ack(right_out_ack)
            if isinstance(is_sync, ControlledZipObservable.SynchronousLeft):
                result_ack_right.connect_ack(is_sync.right_in_ack)
                return result_ack_left
            elif isinstance(is_sync, ControlledZipObservable.SynchronousRight):
                result_ack_left.connect_ack(is_sync.left_in_ack)
                return result_ack_right
            else:
                raise Exception('illegal state')

        elif not has_left_elem:
            if isinstance(is_sync, ControlledZipObservable.SynchronousLeft):
                right_in_ack = is_sync.right_in_ack
            elif isinstance(is_sync, ControlledZipObservable.SynchronousRight):
                right_in_ack = Ack()
            else:
                raise Exception('illegal state')

            next_state = ControlledZipObservable.WaitOnLeft(right_val=right_val, right_iter=right_iter, right_in_ack=right_in_ack,
                                    right_out_ack=right_out_ack)
            self.state = next_state

            result_left_ack = zip_out_ack.merge_ack(left_out_ack)
            if isinstance(is_sync, ControlledZipObservable.SynchronousLeft):
                return result_left_ack
            elif isinstance(is_sync, ControlledZipObservable.SynchronousRight):
                result_left_ack.connect_ack(is_sync.left_in_ack)
                return right_in_ack
            else:
                raise Exception('illegal state')

        elif not has_right_elem:
            if isinstance(is_sync, ControlledZipObservable.SynchronousLeft):
                left_in_ack = Ack()
            elif isinstance(is_sync, ControlledZipObservable.SynchronousRight):
                left_in_ack = is_sync.left_in_ack
            else:
                raise Exception('illegal state')

            next_state = ControlledZipObservable.WaitOnRight(left_val=left_val, left_iter=left_iter, left_in_ack=left_in_ack,
                                    left_out_ack=left_out_ack)
            self.state = next_state

            result_right_ack = zip_out_ack.merge_ack(right_out_ack)
            if isinstance(is_sync, ControlledZipObservable.SynchronousLeft):
                result_right_ack.connect_ack(is_sync.right_in_ack)
                return left_in_ack
            elif isinstance(is_sync, ControlledZipObservable.SynchronousRight):
                return result_right_ack
            else:
                raise Exception('illegal state')

        else:
            raise Exception('illegal case')

    def on_next_left(self, left_elem: Callable[[], Generator]):

        left_iter = left_elem()
        left_val = next(left_iter)

        with self.lock:
            if isinstance(self.state, ControlledZipObservable.WaitOnLeft):
                # iterate over right elements and send them over the publish subject and right observer
                pass

            elif isinstance(self.state, ControlledZipObservable.Completed):
                return stop_ack

            elif isinstance(self.state, ControlledZipObservable.WaitLeftRight):
                # wait until right iterable is received
                left_in_ack = Ack()
                new_state = ControlledZipObservable.WaitOnRight(left_val=left_val, left_iter=left_iter,
                                        left_in_ack=left_in_ack, left_out_ack=None)
                self.state = new_state
                return left_in_ack

            else:
                raise Exception('illegal state {}'.format(self.state))

            # save current state to local variable
            current_state: ControlledZipObservable.WaitOnLeft = self.state

            # change state
            self.state = ControlledZipObservable.Transition()

        right_val = current_state.right_val
        right_iter = current_state.right_iter
        right_in_ack = current_state.right_in_ack
        right_out_ack = current_state.right_out_ack

        is_sync = ControlledZipObservable.SynchronousLeft(right_in_ack=right_in_ack)

        left_ack = self.iterate_over_batch(
            left_val=left_val,
            left_iter=left_iter, last_left_out_ack=None,
            right_val=right_val, right_iter=right_iter,
            is_sync=is_sync, last_right_out_ack=right_out_ack)

        return left_ack

    def on_next_right(self, right_elem: Callable[[], Generator]):

        # create the right iterable
        right_iter = right_elem()
        right_val = next(right_iter)

        with self.lock:
            if isinstance(self.state, ControlledZipObservable.WaitOnRight):
                pass
            elif isinstance(self.state, ControlledZipObservable.Completed):
                return stop_ack
            elif isinstance(self.state, ControlledZipObservable.WaitLeftRight):
                right_ack = Ack()
                new_state = ControlledZipObservable.WaitOnLeft(right_val=right_val, right_iter=right_iter,
                                       right_in_ack=right_ack, right_out_ack=None)
                self.state = new_state
                return right_ack
            else:
                raise Exception('illegal state {}'.format(self.state))

            current_state: ControlledZipObservable.WaitOnRight = self.state
            self.state = ControlledZipObservable.Transition()

        left_val = current_state.left_val
        left_iter = current_state.left_iter
        left_in_ack = current_state.left_in_ack
        left_out_ack = current_state.left_out_ack

        right_ack = self.iterate_over_batch(
            left_val=left_val, left_iter=left_iter,
            last_left_out_ack=left_out_ack,
            right_val=right_val, right_iter=right_iter, last_right_out_ack=None,
            is_sync=ControlledZipObservable.SynchronousRight(left_in_ack=left_in_ack))

        return right_ack

    def on_error(self, exc):
        forward_error = False

        with self.lock:
            if self.exception is not None:
                return
            elif isinstance(self.state, ControlledZipObservable.Transition):
                pass
            else:
                forward_error = True
                self.state = ControlledZipObservable.Completed()

            self.exception = exc

        if forward_error:
            self.left_selector.on_error(exc)
            self.right_selector.on_error(exc)

    def on_completed_left(self):
        complete = False

        with self.lock:
            if self.left_completed:
                return
            elif isinstance(self.state, ControlledZipObservable.WaitLeftRight) or isinstance(self.state,
                                                                                               ControlledZipObservable.WaitOnLeft):
                complete = True
                self.state = ControlledZipObservable.Completed()
            else:
                pass

            self.left_completed = True

        if complete:
            self.left_selector.on_completed()
            self.right_selector.on_completed()
            self.observer.on_completed()

    def on_completed_right(self):
        complete = False

        with self.lock:
            if self.right_completed:
                return
            elif isinstance(self.state, ControlledZipObservable.WaitLeftRight) or isinstance(self.state,
                                                                                               ControlledZipObservable.WaitOnRight):
                complete = True
                self.state = ControlledZipObservable.Completed()
            else:
                pass

            self.right_completed = True

        if complete:
            self.left_selector.on_completed()
            self.right_selector.on_completed()
            self.observer.on_completed()

    def observe(self, observer: Observer):
        self.observer = observer

        left_observer2 = AnonymousObserver(self.on_next_left, self.on_error, self.on_completed_left)
        d1 = self.left_observable.observe(left_observer2)

        right_observer2 = AnonymousObserver(self.on_next_right, self.on_error, self.on_completed_right)
        d2 = self.right_observable.observe(right_observer2)

        return CompositeDisposable(d1, d2)
