import itertools
from typing import Callable, Any, Generator, List, Iterator, Tuple, Optional

from rx import config
from rx.disposables import CompositeDisposable

from rxbackpressurebatched.ack import Stop, Continue, Ack, continue_ack, stop_ack
from rxbackpressurebatched.observable import Observable
from rxbackpressurebatched.observer import Observer
from rxbackpressurebatched.scheduler import Scheduler
from rxbackpressurebatched.subjects.publishsubject import PublishSubject


def window(left: Observable, right: Observable,
                 is_lower: Callable[[Any, Any], bool],
                 is_higher: Callable[[Any, Any], bool]):
    """
    :param left:
    :param right:
    :param is_lower: if right is lower than left, request next right
    :param is_higher: if right is higher than left, request next left
    """

    class State:
        pass

    class InitialState(State):
        pass

    class WaitOnLeft(State):
        def __init__(self,
                     right_val: Any,
                     right_iter: Iterator,
                     right_in_ack: Ack,
                     # last_left_out_ack: Optional[Ack],
                     ):
            self.right_val = right_val
            self.right_iter = right_iter
            self.right_in_ack = right_in_ack
            # self.last_left_out_ack = last_left_out_ack

    class WaitOnRight(State):
        def __init__(self,
                     left_iter: Iterator,
                     first_left: Any,
                     publish_subject: PublishSubject,
                     left_in_ack: Ack,
                     last_left_out_ack: Optional[Ack]):
            self.first_left = first_left
            self.publish_subject = publish_subject
            self.left_iter = left_iter
            self.left_in_ack = left_in_ack
            self.last_left_out_ack = last_left_out_ack

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

        def __init__(self, left_in_ack: Ack, left_out_ack: Ack):
            self.left_in_ack = left_in_ack
            self.left_out_ack = left_out_ack

    class Asynchronous(ConcurrentType):
        """ Object indicating if function is called asynchronously
        """

        def __init__(self, left_in_ack: Ack, last_left_out_ack: Optional[Ack],
                     right_in_ack: Ack):
            self.left_in_ack = left_in_ack
            self.last_left_out_ack = last_left_out_ack
            self.right_in_ack = right_in_ack

    class OnLeftLowerState:
        pass

    class SyncMoreLeftOLL(OnLeftLowerState):
        """ on_left_lower is called synchronously from iterate_over_right
        At least one left value is lower than current right value,
        there are possible more left values in iterable.
        """

        def __init__(self, left_val, left_iter, last_left_out_ack):
            self.left_val = left_val
            self.left_iter = left_iter
            self.last_left_out_ack = last_left_out_ack

    class SyncNoMoreLeftOLL(OnLeftLowerState):
        """ on_left_lower is called synchronously from iterate_over_right
        No left value is lower than current right value, left iterable is empty
        """

        def __init__(self, left_in_ack, right_in_ack):
            self.left_in_ack = left_in_ack
            self.right_in_ack = right_in_ack

    class AsyncOLL(OnLeftLowerState):
        """ Crossing asynchronous border when sending left values
        """

        def __init__(self, left_in_ack, right_in_ack):
            self.left_in_ack = left_in_ack
            self.right_in_ack = right_in_ack

    left_is_higher = is_lower
    left_is_lower = is_higher
    right_is_higher = is_lower
    right_is_lower = is_higher

    def unsafe_subscribe(scheduler: Scheduler, subscribe_scheduler: Scheduler):
        exception = [None]
        left_completed = [False]
        right_completed = [False]

        state = [InitialState()]
        lock = config['concurrency'].RLock()

        def on_left_lower(left_iter: Iterator[Any], last_left_out_ack: Optional[Ack],
                          right_val: Any, right_iter: Iterator[Any],
                          is_sync: ConcurrentType) \
                -> OnLeftLowerState:
            """

            :param left_iter:
            :param last_left_out_ack:
            :param right_val:
            :param right_iter:
            :param right_ack: last right ack
            :param is_sync: is this function called synchronous from on_next_left
            :return:
            """

            # # initialize left_out_ack before while looop
            # left_out_ack = last_left_out_ack

            # iterate over left until left is not lower than right
            while True:

                # check if there is a next left element
                try:
                    next_left_val = next(left_iter)
                    has_next = True
                except StopIteration:
                    next_left_val = None
                    has_next = False

                if has_next:
                    # there is a next left value, which can be send to left observer

                    # print('last left out ack = {}'.format(last_left_out_ack))
                    if last_left_out_ack is None or isinstance(last_left_out_ack, Continue):
                        # synchronous case

                        publish_subject = PublishSubject()
                        last_left_out_ack = left_observer[0].on_next((next_left_val, publish_subject))

                        # if left value is lower than right value, then go to next left value
                        if left_is_lower(next_left_val, right_val):
                            # go to next left element
                            pass

                        elif isinstance(is_sync, SynchronousLeft) or isinstance(is_sync, SynchronousRight):
                            return SyncMoreLeftOLL(left_val=(next_left_val, publish_subject),
                                                    left_iter=left_iter, last_left_out_ack=last_left_out_ack)
                        elif isinstance(is_sync, Asynchronous):
                            # continue iterating over right parameters
                            # print('iterate over right 1')
                            iterate_over_right(left_val=next_left_val, left_iter=left_iter,
                                               last_left_out_ack=last_left_out_ack,
                                               publish_subject=publish_subject,
                                               right_val=right_val, right_iter=right_iter,
                                               is_sync=is_sync)
                            # print('iterate over right 2')

                            return None
                        else:
                            raise Exception('illegal case')

                    elif isinstance(last_left_out_ack, Stop):
                        raise NotImplementedError
                    else:
                        if isinstance(is_sync, SynchronousLeft):
                            # switch to asynchronous call
                            left_in_ack = Ack()
                            right_in_ack = is_sync.right_in_ack
                            updated_is_sync = Asynchronous(left_in_ack=left_in_ack,
                                                           right_in_ack=right_in_ack,
                                                           last_left_out_ack=last_left_out_ack)
                        elif isinstance(is_sync, SynchronousRight):
                            # switch to asynchronous call
                            right_in_ack = Ack()
                            left_in_ack = is_sync.left_in_ack
                            updated_is_sync = Asynchronous(left_in_ack=left_in_ack,
                                                           right_in_ack=right_in_ack,
                                                           last_left_out_ack=last_left_out_ack)
                        elif isinstance(is_sync, Asynchronous):
                            left_in_ack = is_sync.left_in_ack
                            right_in_ack = is_sync.right_in_ack
                            updated_is_sync = is_sync
                        else:
                            raise Exception('illegal case')

                        def _(v):
                            if isinstance(v, Continue):
                                # asynchronous call
                                updated_first_iter = itertools.chain([next_left_val], left_iter)
                                on_left_lower(left_iter=updated_first_iter, last_left_out_ack=v,
                                              right_val=right_val, right_iter=right_iter,
                                              is_sync=updated_is_sync)
                            else:
                                raise NotImplementedError

                        last_left_out_ack.observe_on(scheduler).subscribe(_)
                        return AsyncOLL(left_in_ack=left_in_ack, right_in_ack=right_in_ack)
                else:
                    # left iter is empty

                    if isinstance(is_sync, SynchronousLeft) or isinstance(is_sync, Asynchronous):
                        right_in_ack = is_sync.right_in_ack
                    elif isinstance(is_sync, SynchronousRight):
                        right_in_ack = Ack()
                    else:
                        raise Exception('illegal case')

                    with lock:
                        if left_completed[0]:
                            # complete observer
                            right_observer[0].on_completed()
                            left_observer[0].on_completed()
                            return_stop_ack = True
                        elif exception[0] is not None:
                            right_observer[0].on_error(exception[0])
                            left_observer[0].on_error(exception[0])
                            return_stop_ack = True
                        else:
                            new_state = WaitOnLeft(right_val=right_val,
                                                   right_iter=right_iter,
                                                   right_in_ack=right_in_ack,)
                                                   # last_left_out_ack=last_left_out_ack)
                            state[0] = new_state
                            return_stop_ack = False

                    if return_stop_ack:
                        left_in_ack = stop_ack
                    else:
                        left_in_ack = continue_ack

                    if isinstance(is_sync, SynchronousLeft):
                        return SyncNoMoreLeftOLL(left_in_ack=left_in_ack, right_in_ack=right_in_ack)
                    elif isinstance(is_sync, SynchronousRight) or isinstance(is_sync, Asynchronous):
                        if return_stop_ack or last_left_out_ack is None:
                            is_sync.left_in_ack.on_next(left_in_ack)
                            is_sync.left_in_ack.on_completed()
                        else:
                            last_left_out_ack.connect_ack(is_sync.left_in_ack)
                        return SyncNoMoreLeftOLL(left_in_ack=left_in_ack, right_in_ack=right_in_ack)
                    else:
                        raise Exception('illegal case')

        def iterate_over_right(left_val: Any, left_iter: Iterator[Any],
                               last_left_out_ack: Optional[Ack],
                               publish_subject: PublishSubject,
                               right_val: Optional[Any], right_iter: Iterator[Any],
                               is_sync: ConcurrentType) \
                -> Ack:
            """
            Gets called
            - case1: from on_next_left: first left is guaranteed
            - case2: left is smaller than right, there is another left, left goes asynchronous
            - ??new synchronous left is smaller than right, there is another left val, last right is asynchronous,
                current right is sent

            :param left_val: first left value (first element of left_iter)
            :param left_iter: left iterable (received by on_next_left)
            :param last_left_out_ack: only used in case2
            :param right_iter:
            :param right_index_buffer:
            :param right_val_buffer:
            :param publish_subject:
            :param right_ack:
            :return:
            """

            # print('iterate over right')

            # buffer representing which right elements are selected, and which are not
            right_index_buffer = []

            # buffer containing the values to be send over the publish subject
            right_val_buffer = []

            while True:

                # print('left_val={}, right_val={}'.format(left_val, right_val))

                if left_is_lower(left_val, right_val):
                    # right is higher than left

                    # send right elements (collected in a list) to inner observer
                    if right_val_buffer:
                        def gen_right():
                            yield from right_val_buffer

                        # ignore acknowledment because observer is completed right away
                        _ = publish_subject.on_next(gen_right)

                    # complete inner observable
                    publish_subject.on_completed()

                    oll_state: OnLeftLowerState = on_left_lower(
                        left_iter=left_iter, last_left_out_ack=last_left_out_ack,
                        right_val=right_val, right_iter=right_iter,
                        is_sync=is_sync)

                    if isinstance(oll_state, SyncMoreLeftOLL):
                        # continue to iterate through right iterable

                        oll_state: SyncMoreLeftOLL = oll_state

                        left_val = oll_state.left_val
                        left_iter = oll_state.left_iter
                        last_left_out_ack = oll_state.last_left_out_ack
                    elif isinstance(oll_state, SyncNoMoreLeftOLL):
                        # request new left iterable

                        # there are no left values left, request new left value
                        if isinstance(is_sync, SynchronousLeft):
                            # is_empty(left iterable) can only be checked if ack has value
                            return oll_state.left_in_ack
                        elif isinstance(is_sync, SynchronousRight):
                            return oll_state.right_in_ack
                        elif isinstance(is_sync, Asynchronous):
                            return None
                        else:
                            raise Exception('illegal case')

                    elif isinstance(oll_state, AsyncOLL):
                        # crossed asynchronous border

                        # there are no left values left, request new left value
                        if isinstance(is_sync, SynchronousLeft):
                            return oll_state.left_in_ack
                        elif isinstance(is_sync, SynchronousRight):
                            return oll_state.right_in_ack
                        elif isinstance(is_sync, Asynchronous):
                            return None
                        else:
                            raise Exception('illegal case')
                    else:
                        raise Exception('illegal case')

                if left_is_higher(left_val, right_val):
                    # update right index
                    right_index_buffer.append((False, right_val))

                else:
                    # update right index
                    right_index_buffer.append((True, right_val))

                    # add to buffer
                    right_val_buffer.append(right_val)

                # check if there is a next right element
                try:
                    right_val = next(right_iter)
                    has_next = True
                except StopIteration:
                    has_next = False

                if has_next:
                    pass
                else:
                    break

            # getting to this point in the code means that there are no more elements in the right iterable, request
            # a new right immediately

            # case SynchronousLeft - generate new left_ack and save it in state, send continue to right_ack
            # case SynchronousRight - return continue_ack
            # case Asynchronous - save left_ack to state, send continue to right_ack

            if right_val_buffer:
                def gen_right_items_to_inner():
                    yield from right_val_buffer

                inner_ack = publish_subject.on_next(gen_right_items_to_inner)
            else:
                inner_ack = continue_ack

            if right_index_buffer:
                def gen_right_index():
                    yield from right_index_buffer

                right_out_ack = right_observer[0].on_next(gen_right_index)
            else:
                return  continue_ack

            if isinstance(is_sync, SynchronousLeft):
                # create a new left in ack
                # because is_sync is SynchronousLeft there has been the possibility that
                # left can be directly back-pressured with continue left

                left_in_ack = Ack()
            elif isinstance(is_sync, SynchronousRight):
                left_in_ack = is_sync.left_in_ack
            elif isinstance(is_sync, Asynchronous):
                left_in_ack = is_sync.left_in_ack
            else:
                raise Exception('illegal case')

            with lock:
                if not right_completed[0] and exception[0] is None:
                    new_state = WaitOnRight(first_left=left_val, publish_subject=publish_subject,
                                            left_iter=left_iter,
                                            left_in_ack=left_in_ack,
                                            last_left_out_ack=last_left_out_ack)
                    state[0] = new_state

            if right_completed[0]:
                publish_subject.on_completed()
                right_observer[0].on_completed()
                left_observer[0].on_completed()
                return stop_ack
            elif exception[0] is not None:
                right_observer[0].on_error(exception[0])
                left_observer[0].on_error(exception[0])
                return stop_ack

            # only request a new right, if previous inner and right ack are done
            ack = inner_ack.merge_ack(right_out_ack)

            # right_in_ack gets directly connected to right_out_ack, therefore it
            # does not need to be kept in memory any longer
            if isinstance(is_sync, SynchronousRight):
                return ack
            else:
                right_in_ack = is_sync.right_in_ack
                # print(right_in_ack)
                ack.connect_ack(right_in_ack)

            if isinstance(is_sync, SynchronousLeft):
                return left_in_ack

        def on_next_left(left_elem: Callable[[], Generator]):
            # print('window on left')

            # create the left iterable
            left_iter = left_elem()

            # by convention, there is at least one element in the left iterable
            left_item = next(left_iter)

            # send first left item
            publish_subject = PublishSubject()
            last_left_out_ack = left_observer[0].on_next((left_item, publish_subject))

            with lock:
                if isinstance(state[0], WaitOnLeft):
                    # iterate over right elements and send them over the publish subject and right observer
                    pass
                elif isinstance(state[0], Completed):
                    return stop_ack
                elif isinstance(state[0], InitialState):
                    # wait until right iterable is received
                    left_in_ack = Ack()
                    new_state = WaitOnRight(first_left=left_item,
                                            publish_subject=publish_subject,
                                            left_iter=left_iter,
                                            left_in_ack=left_in_ack,
                                            last_left_out_ack=last_left_out_ack)
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
            # last_left_out_ack = current_state.last_left_out_ack

            # case 1: state changed to WaitOnRight, returns acknowledgment from left, and right and inner right connected
            # case 2: state changed to WaitOnLeft, returns acknowledgment from left
            # case 3: state doesn't change, returns None
            is_sync = SynchronousLeft(right_in_ack=right_in_ack)

            left_ack = iterate_over_right(
                left_val=left_item, left_iter=left_iter,
                last_left_out_ack=last_left_out_ack,
                publish_subject=publish_subject,
                right_val=right_val, right_iter=right_iter,
                is_sync=is_sync)

            return left_ack

        def on_next_right(right_elem: Callable[[], Generator]):
            # print('window on right')
            # create the right iterable
            right_iter = right_elem()
            right_val = next(right_iter)

            with lock:
                if isinstance(state[0], WaitOnRight):
                    pass
                elif isinstance(state[0], Completed):
                    return stop_ack
                elif isinstance(state[0], InitialState):
                    right_ack = Ack()
                    new_state = WaitOnLeft(right_val=None, right_iter=right_iter,
                                           right_in_ack=right_ack) #, last_left_out_ack=None)
                    state[0] = new_state
                    return right_ack
                else:
                    raise Exception('illegal state {}'.format(state[0]))

                current_state: WaitOnRight = state[0]
                state[0] = Transition()

            left_val = current_state.first_left
            publish_subject = current_state.publish_subject
            left_iter = current_state.left_iter
            left_in_ack = current_state.left_in_ack
            left_out_ack = current_state.last_left_out_ack
            right_ack = iterate_over_right(
                left_val=left_val, left_iter=left_iter, last_left_out_ack=left_out_ack,
                publish_subject=publish_subject,
                right_val=right_val, right_iter=right_iter,
                is_sync=SynchronousRight(left_in_ack=left_in_ack, left_out_ack=left_out_ack))

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
                complete = False

                with lock:
                    if left_completed[0]:
                        return
                    elif isinstance(state[0], InitialState) or isinstance(state[0], WaitOnLeft):
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
                    elif isinstance(state[0], InitialState) or isinstance(state[0], WaitOnRight):
                        complete = True
                        state[0] = Completed()
                    else:
                        pass

                    right_completed[0] = True

                if complete:
                    left_observer[0].on_completed()
                    right_observer[0].on_completed()

        left_observer2 = LeftObserver()
        d1 = left.unsafe_subscribe(left_observer2, scheduler, subscribe_scheduler)

        right_observer2 = RightObserver()
        d2 = right.unsafe_subscribe(right_observer2, scheduler, subscribe_scheduler)

        return CompositeDisposable(d1, d2)

    class DummyObserver(Observer):
        def on_next(self, v):
            return continue_ack

        def on_error(self, err):
            pass

        def on_completed(self):
            pass

    lock = config['concurrency'].RLock()

    left_observer = [DummyObserver()]
    right_observer = [DummyObserver()]

    class LeftObservable(Observable):
        def unsafe_subscribe(self, observer, scheduler, s):
            with lock:
                left_observer[0] = observer

                if isinstance(right_observer[0], DummyObserver):
                    subscribe = True
                else:
                    subscribe = False

            if subscribe:
                unsafe_subscribe(scheduler, s)

    o1 = LeftObservable()

    class RightObservable(Observable):
        def unsafe_subscribe(self, observer, scheduler, s):
            with lock:
                right_observer[0] = observer

                if isinstance(left_observer[0], DummyObserver):
                    subscribe = True
                else:
                    subscribe = False

            if subscribe:
                unsafe_subscribe(scheduler, s)

    o2 = RightObservable()

    return o1, o2
