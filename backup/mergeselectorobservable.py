import itertools
import threading
import traceback
from typing import Callable, Any, Generator, List, Iterator, Tuple, Optional, Iterable


from rx.disposable import CompositeDisposable

from rxbp.ack import Stop, Continue, Ack, continue_ack, stop_ack
from rxbp.internal.selection import SelectCompleted, SelectNext, select_next, select_completed, Selection
from rxbp.observable import Observable
from rxbp.observer import Observer
from rxbp.scheduler import Scheduler
from rxbp.subjects.publishsubject import PublishSubject


def merge_selector(left: Observable, right: Observable):
    """
    :param left:
    :param right:
    """

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

    # left_is_higher = is_higher
    # left_is_lower = is_lower

    def observe(observer: Observer): #scheduler: Scheduler, subscribe_scheduler: Scheduler):
        exception = [None]
        left_completed = [False]
        right_completed = [False]

        state = [WaitLeftRight()]
        lock = threading.RLock()

        def start_zipping(left_val: Any,
                          last_left_out_ack: Optional[Ack], left_iter: Iterator[Tuple[Any, PublishSubject]],
                          right_val: Optional[Any], right_iter: Iterator[Any], last_right_out_ack: Optional[Ack],
                          is_sync: ConcurrentType) \
                -> Ack:

            # buffer representing which right elements are selected, and which are not
            left_index_buffer = []
            right_index_buffer = []

            zipped_output_buffer = []

            has_left_elem = True
            has_right_elem = True

            while True:

                print('left_val={}, right_val={}'.format(left_val, right_val))

                match_func = lambda l, r: isinstance(r, select_next)
                request_left = lambda l, r: isinstance(r, select_completed)

                if match_func(left_val, right_val):
                    left_index_buffer.append(select_next)
                    right_index_buffer.append(select_next)

                    # add to buffer
                    zipped_output_buffer.append((left_val, right_val))

                left_requested = False
                if request_left(left_val, right_val):
                    # print('left is lower, right_val_buffer = {}'.format(right_val_buffer[0]))

                    left_index_buffer.append(select_completed)
                    try:
                        new_left_val = next(left_iter)
                        left_requested = True
                    except StopIteration:
                        has_left_elem = False

                if True:
                    # update right index
                    right_index_buffer.append(select_completed)

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

                zip_out_ack = controller_zip_observer[0].on_next(gen)
            else:
                zip_out_ack = continue_ack

            if left_index_buffer:
                def gen():
                    yield from left_index_buffer

                left_out_ack = left_observer[0].on_next(gen)
            else:
                left_out_ack = last_left_out_ack or continue_ack

            if right_index_buffer:
                def gen():
                    yield from right_index_buffer

                right_out_ack = right_observer[0].on_next(gen)
            else:
                right_out_ack = last_right_out_ack or continue_ack

            if not has_left_elem and not has_right_elem:
                next_state = WaitLeftRight()
                state[0] = next_state

                result_ack_left = zip_out_ack.merge_ack(left_out_ack)
                result_ack_right = zip_out_ack.merge_ack(right_out_ack)
                if isinstance(is_sync, SynchronousLeft):
                    result_ack_right.connect_ack(is_sync.right_in_ack)
                    return result_ack_left
                elif isinstance(is_sync, SynchronousRight):
                    result_ack_left.connect_ack(is_sync.left_in_ack)
                    return result_ack_right
                else:
                    raise Exception('illegal state')

            elif not has_left_elem:
                if isinstance(is_sync, SynchronousLeft):
                    right_in_ack = is_sync.right_in_ack
                elif isinstance(is_sync, SynchronousRight):
                    right_in_ack = Ack()
                else:
                    raise Exception('illegal state')

                next_state = WaitOnLeft(right_val=right_val, right_iter=right_iter, right_in_ack=right_in_ack,
                                        right_out_ack=right_out_ack)
                state[0] = next_state

                result_left_ack = zip_out_ack.merge_ack(left_out_ack)
                if isinstance(is_sync, SynchronousLeft):
                    return result_left_ack
                elif isinstance(is_sync, SynchronousRight):
                    result_left_ack.connect_ack(is_sync.left_in_ack)
                    return right_in_ack
                else:
                    raise Exception('illegal state')

            elif not has_right_elem:
                if isinstance(is_sync, SynchronousLeft):
                    left_in_ack = Ack()
                elif isinstance(is_sync, SynchronousRight):
                    left_in_ack = is_sync.left_in_ack
                else:
                    raise Exception('illegal state')

                next_state = WaitOnRight(left_val=left_val, left_iter=left_iter, left_in_ack=left_in_ack,
                                        left_out_ack=left_out_ack)
                state[0] = next_state

                result_right_ack = zip_out_ack.merge_ack(right_out_ack)
                if isinstance(is_sync, SynchronousLeft):
                    result_right_ack.connect_ack(is_sync.right_in_ack)
                    return left_in_ack
                elif isinstance(is_sync, SynchronousRight):
                    return result_right_ack
                else:
                    raise Exception('illegal state')

            else:
                raise Exception('illegal case')

        def get_continue_index(indexes: Callable[[], Generator]):
            val_iter = indexes()
            has_values = [True]
            last_val = [next(val_iter)]

            if isinstance(last_val[0], select_completed):
                has_values[0] = False
                def gen1():
                    yield last_val[0]

                    for val in val_iter:
                        if isinstance(val, select_completed):
                            yield val
                        else:
                            has_values[0] = True
                            last_val[0] = val
                            break
            else:
                gen1 = None

            if has_values[0]:
                def gen2():
                    yield last_val[0]
                    for val in val_iter:
                        yield val
            else:
                gen2 = None

            return gen1, gen2

        def on_next_left(left_elem: Callable[[], Generator]):
            # print('controlled_zip on left')

            def on_next_filtered():
                left_elem = gen2

                left_iter = left_elem()
                left_val = next(left_iter)

                with lock:
                    if isinstance(state[0], WaitOnLeft):
                        # iterate over right elements and send them over the publish subject and right observer
                        pass

                    elif isinstance(state[0], Completed):
                        return stop_ack

                    elif isinstance(state[0], WaitLeftRight):
                        # wait until right iterable is received
                        left_in_ack = Ack()
                        new_state = WaitOnRight(left_val=left_val, left_iter=left_iter,
                                                left_in_ack=left_in_ack, left_out_ack=None)
                        state[0] = new_state
                        return left_in_ack

                    else:
                        raise Exception('illegal state {}'.format(state[0]))

                    # save current state to local variable
                    current_state: WaitOnLeft = state[0]

                    # change state
                    state[0] = Transition()

                right_val = current_state.right_val
                right_iter = current_state.right_iter
                right_in_ack = current_state.right_in_ack
                right_out_ack = current_state.right_out_ack

                is_sync = SynchronousLeft(right_in_ack=right_in_ack)

                left_ack = start_zipping(
                    left_val=left_val,
                    left_iter=left_iter, last_left_out_ack=None,
                    right_val=right_val, right_iter=right_iter,
                    is_sync=is_sync, last_right_out_ack=right_out_ack)

                return left_ack

            gen1, gen2 = get_continue_index(left_elem)

            if gen1 is not None:
                ack1 = observer.on_next(gen1)

                if gen2 is None:
                    return ack1
                else:
                    ack = Ack()

                    def _(v):
                        ack2 = on_next_filtered()
                        ack2.connect_ack(ack)

                    ack1.subscribe(_)
                    return ack
            else:
                return on_next_filtered()

        def on_next_right(right_elem: Callable[[], Generator]):
            # print('controlled_zip on right')

            # create the right iterable
            right_iter = right_elem()
            right_val = next(right_iter)

            with lock:
                if isinstance(state[0], WaitOnRight):
                    pass
                elif isinstance(state[0], Completed):
                    return stop_ack
                elif isinstance(state[0], WaitLeftRight):
                    right_ack = Ack()
                    new_state = WaitOnLeft(right_val=right_val, right_iter=right_iter,
                                           right_in_ack=right_ack, right_out_ack=None)
                    state[0] = new_state
                    return right_ack
                else:
                    raise Exception('illegal state {}'.format(state[0]))

                current_state: WaitOnRight = state[0]
                state[0] = Transition()

            left_val = current_state.left_val
            left_iter = current_state.left_iter
            left_in_ack = current_state.left_in_ack
            left_out_ack = current_state.left_out_ack

            right_ack = start_zipping(
                left_val=left_val, left_iter=left_iter,
                last_left_out_ack=left_out_ack,
                right_val=right_val, right_iter=right_iter, last_right_out_ack=None,
                is_sync=SynchronousRight(left_in_ack=left_in_ack))

            return right_ack

        def on_error(exc):
            forward_error = False

            with lock:
                if exception[0] is not None:
                    return
                elif isinstance(state[0], Transition):
                    pass
                else:
                    forward_error = True
                    state[0] = Completed()

                exception[0] = exc

            if forward_error:
                right_observer[0].on_error(exc)
                left_observer[0].on_error(exc)

        class LeftObserver(Observer):
            def on_next(self, v):
                return on_next_left(v)

            def on_error(self, exc):
                on_error(exc)

            def on_completed(self):
                # print('left completed')
                complete = False

                with lock:
                    if left_completed[0]:
                        return
                    elif isinstance(state[0], WaitLeftRight) or isinstance(state[0], WaitOnLeft):
                        complete = True
                        state[0] = Completed()
                    else:
                        pass

                    left_completed[0] = True

                if complete:
                    left_observer[0].on_completed()
                    right_observer[0].on_completed()

        class RightObserver(Observer):
            def on_next(self, v):
                return on_next_right(v)

            def on_error(self, exc):
                on_error(exc)

            def on_completed(self):
                complete = False

                with lock:
                    if right_completed[0]:
                        return
                    elif isinstance(state[0], WaitLeftRight) or isinstance(state[0], WaitOnRight):
                        complete = True
                        state[0] = Completed()
                    else:
                        pass

                    right_completed[0] = True

                if complete:
                    left_observer[0].on_completed()
                    right_observer[0].on_completed()

        left_observer2 = LeftObserver()
        d1 = left.observe(left_observer2)  #, scheduler, subscribe_scheduler)

        right_observer2 = RightObserver()
        d2 = right.observe(right_observer2)  #, scheduler, subscribe_scheduler)

        return CompositeDisposable(d1, d2)

    class DummyObserver(Observer):
        def on_next(self, v):
            return continue_ack

        def on_error(self, err):
            pass

        def on_completed(self):
            pass

    lock = threading.RLock()

    controller_zip_observer = [DummyObserver()]
    left_observer = [DummyObserver()]
    right_observer = [DummyObserver()]
    composite_disposable = CompositeDisposable()

    class ControlledZippedObservable(Observable):
        def observe(self, observer): #, scheduler, s):
            with lock:
                controller_zip_observer[0] = observer

                if isinstance(left_observer[0], DummyObserver) and isinstance(right_observer[0], DummyObserver):
                    subscribe = True
                else:
                    subscribe = False

            if subscribe:
                disposable = observe() #scheduler, s)
                composite_disposable.add(disposable)

            return composite_disposable

    o1 = ControlledZippedObservable()

    class LeftObservable(Observable):
        def observe(self, observer): #, scheduler, s):
            with lock:
                left_observer[0] = observer

                if isinstance(controller_zip_observer[0], DummyObserver) and isinstance(right_observer[0], DummyObserver):
                    subscribe = True
                else:
                    subscribe = False

            if subscribe:
                disposable = observe() #scheduler, s)
                composite_disposable.add(disposable)

            return composite_disposable

    o2 = LeftObservable()

    class RightObservable(Observable):
        def observe(self, observer): #, scheduler, s):
            with lock:
                right_observer[0] = observer

                if isinstance(controller_zip_observer[0], DummyObserver) and isinstance(left_observer[0], DummyObserver):
                    subscribe = True
                else:
                    subscribe = False

            if subscribe:
                disposable = observe() #scheduler, s)
                composite_disposable.add(disposable)

            return composite_disposable

    o3 = RightObservable()

    return o1 #, o2, o3
