import threading
from typing import Callable, Any, Generator, Iterator, Tuple, Optional

from rx.disposable import CompositeDisposable
from rxbp.ack.ackimpl import continue_ack, stop_ack
from rxbp.ack.ackbase import AckBase
from rxbp.ack.acksubject import AckSubject
from rxbp.observerinfo import ObserverInfo

from rxbp.selectors.selectionmsg import SelectCompleted, SelectNext
from rxbp.observable import Observable
from rxbp.observer import Observer
from rxbp.scheduler import Scheduler
from rxbp.observablesubjects.publishosubject import PublishOSubject


class MergeSelectorObservable(Observable):
    def __init__(self, left: Observable, right: Observable,
                 scheduler: Scheduler):
        """
        :param left:
        :param right:
        :param is_lower: if right is lower than left, request next right
        :param is_higher: if right is higher than left, request next left
        """

        self.left_observable = left
        self.right_observable = right

        self.exception = None
        self.left_completed = False
        self.right_completed = False
        self.state = MergeSelectorObservable.WaitLeftRight()

        self.lock = threading.RLock()

    class State:
        pass

    class WaitLeftRight(State):
        pass

    class WaitOnLeft(State):
        def __init__(self,
                     right_val: Any,
                     right_iter: Iterator,
                     right_in_ack: AckBase):
            self.right_val = right_val
            self.right_iter = right_iter
            self.right_in_ack = right_in_ack

    class WaitOnRight(State):
        def __init__(self,
                     left_iter: Iterator,
                     left_val: Any,
                     left_in_ack: AckBase):
            self.left_val = left_val
            self.left_iter = left_iter
            self.left_in_ack = left_in_ack

    class Transition(State):
        pass

    class Completed(State):
        pass

    class ConcurrentType:
        pass

    class SynchronousLeft(ConcurrentType):
        """ Object indicating if function is called synchronously
        """

        def __init__(self, right_in_ack: AckBase):
            self.right_in_ack = right_in_ack

    class SynchronousRight(ConcurrentType):
        """ Object indicating if function is called synchronously
        """

        def __init__(self, left_in_ack: AckBase):
            self.left_in_ack = left_in_ack

    def observe(self, observer_info: ObserverInfo):
        observer = observer_info.observer

        def start_zipping(left_val: Any, left_iter: Iterator[Tuple[Any, PublishOSubject]],
                          right_val: Optional[Any], right_iter: Iterator[Any],
                          is_sync: MergeSelectorObservable.ConcurrentType) \
                -> AckBase:

            zipped_output_buffer = []

            has_left_elem = True
            has_right_elem = True

            while True:

                # print('left_val={}, right_val={}'.format(left_val, right_val))

                if isinstance(right_val, SelectNext):
                    # add to buffer
                    zipped_output_buffer.append(left_val)

                left_requested = False
                if isinstance(right_val, SelectCompleted):
                    # print('left is lower, right_val_buffer = {}'.format(right_val_buffer[0]))

                    while True:
                        try:
                            new_left_val = next(left_iter)

                            if isinstance(new_left_val, SelectCompleted):
                                zipped_output_buffer.append(new_left_val)
                            else:
                                left_requested = True
                                break
                        except StopIteration:
                            has_left_elem = False
                            break

                try:
                    right_val = next(right_iter)
                except StopIteration:
                    has_right_elem = False
                    break

                if not has_left_elem:
                    break

                if left_requested:
                    left_val = new_left_val

            if zipped_output_buffer:
                def gen():
                    yield from zipped_output_buffer

                zip_out_ack = observer.on_next(gen)
            else:
                zip_out_ack = continue_ack

            if not has_left_elem and not has_right_elem:
                next_state = MergeSelectorObservable.WaitLeftRight()
                self.state = next_state

                if isinstance(is_sync, MergeSelectorObservable.SynchronousLeft):
                    zip_out_ack.subscribe(is_sync.right_in_ack)
                    return zip_out_ack
                elif isinstance(is_sync, MergeSelectorObservable.SynchronousRight):
                    zip_out_ack.subscribe(is_sync.left_in_ack)
                    return zip_out_ack
                else:
                    raise Exception('illegal state')

            elif not has_left_elem:
                if isinstance(is_sync, MergeSelectorObservable.SynchronousLeft):
                    right_in_ack = is_sync.right_in_ack
                elif isinstance(is_sync, MergeSelectorObservable.SynchronousRight):
                    right_in_ack = AckSubject()
                else:
                    raise Exception('illegal state')

                next_state = MergeSelectorObservable.WaitOnLeft(right_val=right_val, right_iter=right_iter, right_in_ack=right_in_ack)
                self.state = next_state

                if isinstance(is_sync, MergeSelectorObservable.SynchronousLeft):
                    return zip_out_ack
                elif isinstance(is_sync, MergeSelectorObservable.SynchronousRight):
                    zip_out_ack.subscribe(is_sync.left_in_ack)
                    return right_in_ack
                else:
                    raise Exception('illegal state')

            elif not has_right_elem:
                if isinstance(is_sync, MergeSelectorObservable.SynchronousLeft):
                    left_in_ack = AckSubject()
                elif isinstance(is_sync, MergeSelectorObservable.SynchronousRight):
                    left_in_ack = is_sync.left_in_ack
                else:
                    raise Exception('illegal state')

                next_state = MergeSelectorObservable.WaitOnRight(left_val=left_val, left_iter=left_iter, left_in_ack=left_in_ack)
                self.state = next_state

                if isinstance(is_sync, MergeSelectorObservable.SynchronousLeft):
                    zip_out_ack.subscribe(is_sync.right_in_ack)
                    return left_in_ack
                elif isinstance(is_sync, MergeSelectorObservable.SynchronousRight):
                    return zip_out_ack
                else:
                    raise Exception('illegal state')

            else:
                raise Exception('illegal case')

        def on_next_left(left_elem: Callable[[], Generator]):
            # print('controlled_zip on left')

            left_iter = left_elem()
            left_val = [next(left_iter)]

            has_elem = [True]
            def gen_first_continue():
                while True:
                    if isinstance(left_val[0], SelectCompleted):
                        yield left_val[0]
                    else:
                        break

                    try:
                        left_val[0] = next(left_iter)
                    except StopIteration:
                        has_elem[0] = False
                        break

            continue_sel = list(gen_first_continue())

            def continue_processing():
                with self.lock:
                    if isinstance(self.state, MergeSelectorObservable.WaitOnLeft):
                        # iterate over right elements and send them over the publish subject and right observer
                        pass

                    elif isinstance(self.state, MergeSelectorObservable.Completed):
                        return stop_ack

                    elif isinstance(self.state, MergeSelectorObservable.WaitLeftRight):
                        # wait until right iterable is received
                        left_in_ack = AckSubject()
                        new_state = MergeSelectorObservable.WaitOnRight(left_val=left_val[0], left_iter=left_iter,
                                                                        left_in_ack=left_in_ack)
                        self.state = new_state
                        return left_in_ack

                    else:
                        raise Exception('illegal state {}'.format(self.state))

                    # save current state to local variable
                    current_state: MergeSelectorObservable.WaitOnLeft = self.state

                    # change state
                    self.state = MergeSelectorObservable.Transition()

                right_val = current_state.right_val
                right_iter = current_state.right_iter
                right_in_ack = current_state.right_in_ack

                is_sync = MergeSelectorObservable.SynchronousLeft(right_in_ack=right_in_ack)

                left_ack = start_zipping(
                    left_val=left_val[0],
                    left_iter=left_iter,
                    right_val=right_val, right_iter=right_iter,
                    is_sync=is_sync)

                return left_ack

            if continue_sel:
                def gen():
                    yield from continue_sel

                ack = observer.on_next(gen)

                if has_elem[0]:
                    return_ack = AckSubject()

                    def _(v):
                        new_ack = continue_processing()
                        new_ack.subscribe(return_ack)

                    ack.subscribe(_)
                    return return_ack
                else:
                    return ack

            else:
                return continue_processing()

        def on_next_right(right_elem: Callable[[], Generator]):
            # print('controlled_zip on right')

            # create the right iterable
            right_iter = right_elem()
            right_val = next(right_iter)

            with self.lock:
                if isinstance(self.state, MergeSelectorObservable.WaitOnRight):
                    pass
                elif isinstance(self.state, MergeSelectorObservable.Completed):
                    return stop_ack
                elif isinstance(self.state, MergeSelectorObservable.WaitLeftRight):
                    right_ack = AckSubject()
                    new_state = MergeSelectorObservable.WaitOnLeft(right_val=right_val, right_iter=right_iter,
                                                                   right_in_ack=right_ack)
                    self.state = new_state
                    return right_ack
                else:
                    raise Exception('illegal state {}'.format(self.state))

                current_state: MergeSelectorObservable.WaitOnRight = self.state
                self.state = MergeSelectorObservable.Transition()

            left_val = current_state.left_val
            left_iter = current_state.left_iter
            left_in_ack = current_state.left_in_ack

            right_ack = start_zipping(
                left_val=left_val, left_iter=left_iter,
                right_val=right_val, right_iter=right_iter,
                is_sync=MergeSelectorObservable.SynchronousRight(left_in_ack=left_in_ack))

            return right_ack

        source = self

        def on_error(exc):
            forward_error = False

            with self.lock:
                if self.exception is not None:
                    return
                elif isinstance(self.state, MergeSelectorObservable.Transition):
                    pass
                else:
                    forward_error = True
                    self.state = MergeSelectorObservable.Completed()

                self.exception = exc

            if forward_error:
                observer.on_error(exc)

        class LeftObserver(Observer):
            def on_next(self, v):
                return on_next_left(v)

            def on_error(self, exc):
                on_error(exc)

            def on_completed(self):
                # print('left completed')
                complete = False

                with source.lock:
                    if source.left_completed:
                        return
                    elif isinstance(source.state, MergeSelectorObservable.WaitLeftRight) or isinstance(source.state, MergeSelectorObservable.WaitOnLeft):
                        complete = True
                        source.state = MergeSelectorObservable.Completed()
                    else:
                        pass

                    source.left_completed = True

                if complete:
                    observer.on_completed()

        class RightObserver(Observer):
            def on_next(self, v):
                return on_next_right(v)

            def on_error(self, exc):
                on_error(exc)

            def on_completed(self):
                # print('right on_completed')
                complete = False

                with source.lock:
                    if source.right_completed:
                        return
                    elif isinstance(source.state, MergeSelectorObservable.WaitLeftRight) or isinstance(source.state, MergeSelectorObservable.WaitOnRight):
                        complete = True
                        source.state = MergeSelectorObservable.Completed()
                    else:
                        pass

                    source.right_completed = True

                if complete:
                    observer.on_completed()

        left_observer = LeftObserver()
        left_subscription = ObserverInfo(left_observer, is_volatile=observer_info.is_volatile)
        d1 = self.left_observable.observe(left_subscription)

        right_observer = RightObserver()
        right_subscription = ObserverInfo(right_observer, is_volatile=observer_info.is_volatile)
        d2 = self.right_observable.observe(right_subscription)

        return CompositeDisposable(d1, d2)
