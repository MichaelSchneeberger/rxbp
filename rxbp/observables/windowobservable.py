import threading
from typing import Callable, Any, Generator, Iterator, Tuple, Optional


from rx.disposable import CompositeDisposable

from rxbp.ack import Ack, continue_ack, stop_ack
from rxbp.observable import Observable
from rxbp.observables.windowobservablestates import WindowObservableStates
from rxbp.observer import Observer
from rxbp.observers.emptyobserver import EmptyObserver
from rxbp.scheduler import Scheduler
from rxbp.subjects.publishsubject import PublishSubject


class WindowObservable(Observable):
    def __init__(self, left: Observable, right: Observable, right_base: Any,
                 is_lower: Callable[[Any, Any], bool], is_higher: Callable[[Any, Any], bool]):
        """
        :param left: left observable
        :param right: right observable
        :param is_lower: if right is lower than left, request next right
        :param is_higher: if right is higher than left, request next left
        """

        self._left = left
        self._right = right
        self._left_is_higher = is_lower
        self._left_is_lower = is_higher

        self._lock = threading.RLock()
        self._window_observer = EmptyObserver()
        self._index_observer = EmptyObserver()
        self._composite_disposable = CompositeDisposable()

        source = self

        class IndexObservable(Observable):
            def unsafe_subscribe(self, observer, scheduler, subscribe_scheduler):
                with source._lock:
                    source._index_observer = observer

                    if isinstance(source._window_observer, EmptyObserver):
                        subscribe = True
                    else:
                        subscribe = False

                if subscribe:
                    disposable = source._unsafe_subscribe(scheduler, subscribe_scheduler)
                    source._composite_disposable.add(disposable)

                return source._composite_disposable

        self._index_observable = IndexObservable(transformations=right.transformations)

        transformations = {**{right_base: self._index_observable}, **left.transformations}
        super().__init__(transformations=transformations)

    def _unsafe_subscribe(self, scheduler: Scheduler, subscribe_scheduler: Scheduler):
        exception = [None]
        left_completed = [False]
        right_completed = [False]

        state = [WindowObservableStates.InitialState()]
        source = self

        def on_left_lower(val_subject_iter: Iterator[Tuple[Any, PublishSubject]],
                          last_left_out_ack: Optional[Ack],
                          right_val: Any, right_iter: Iterator[Any],
                          is_sync: WindowObservableStates.ConcurrentType) \
                -> WindowObservableStates.OnLeftLowerState:
            """
            :param is_sync: is this function called synchronous from on_next_left
            :return:
            """

            # print('on left lower')

            # iterate over left until left is not lower than right
            while True:

                # check if there is a next left element
                try:
                    next_left_val, subject = next(val_subject_iter)
                    has_next = True
                except StopIteration:
                    next_left_val = None
                    subject = None
                    has_next = False

                if has_next:
                    # there is a next left value, which can be send to left observer

                    # print('next left val: {}'.format(next_left_val))

                    # if left value is lower than right value, then go to next left value
                    if self._left_is_lower(next_left_val, right_val):
                        subject.on_completed()

                    # if in sync mode, return all information to continue oiterating over right values
                    elif isinstance(is_sync, WindowObservableStates.SynchronousLeft) or isinstance(is_sync, WindowObservableStates.SynchronousRight):
                        return WindowObservableStates.SyncMoreLeftOLL(left_val=next_left_val, subject=subject,
                                               last_left_out_ack=last_left_out_ack)

                    elif isinstance(is_sync, WindowObservableStates.Asynchronous):
                        # continue iterating over right value
                        iterate_over_right(left_val=next_left_val, val_subject_iter=val_subject_iter,
                                           last_left_out_ack=last_left_out_ack,
                                           subject=subject,
                                           right_val=right_val, right_iter=right_iter,
                                           is_sync=is_sync)
                        return None
                    else:
                        raise Exception('illegal case')

                # val_subject_iter is empty, request new left
                else:
                    if isinstance(is_sync, WindowObservableStates.SynchronousLeft) or isinstance(is_sync, WindowObservableStates.Asynchronous):
                        right_in_ack = is_sync.right_in_ack

                    elif isinstance(is_sync, WindowObservableStates.SynchronousRight):
                        right_in_ack = Ack()

                    else:
                        raise Exception('illegal case')

                    with self._lock:
                        if left_completed[0]:
                            # complete observer
                            self._index_observer.on_completed()
                            self._window_observer.on_completed()
                            return_stop_ack = True
                        elif exception[0] is not None:
                            self._index_observer.on_error(exception[0])
                            self._window_observer.on_error(exception[0])
                            return_stop_ack = True
                        else:
                            new_state = WindowObservableStates.WaitOnLeft(right_val=right_val,
                                                   right_iter=right_iter,
                                                   right_in_ack=right_in_ack, )
                            state[0] = new_state
                            return_stop_ack = False

                    if return_stop_ack:
                        left_in_ack = stop_ack
                    elif last_left_out_ack:
                        left_in_ack = last_left_out_ack
                    else:
                        left_in_ack = continue_ack  # replace by last out ack

                    if isinstance(is_sync, WindowObservableStates.SynchronousLeft):
                        return WindowObservableStates.SyncNoMoreLeftOLL(left_in_ack=left_in_ack, right_in_ack=right_in_ack)
                    elif isinstance(is_sync, WindowObservableStates.SynchronousRight) or isinstance(is_sync, WindowObservableStates.Asynchronous):
                        if return_stop_ack or last_left_out_ack is None:
                            is_sync.left_in_ack.on_next(left_in_ack)
                            is_sync.left_in_ack.on_completed()
                        else:
                            last_left_out_ack.connect_ack(is_sync.left_in_ack)
                        return WindowObservableStates.SyncNoMoreLeftOLL(left_in_ack=left_in_ack, right_in_ack=right_in_ack)
                    else:
                        raise Exception('illegal case')

        def iterate_over_right(left_val: Any,
                               last_left_out_ack: Optional[Ack],
                               subject: PublishSubject, val_subject_iter: Iterator[Tuple[Any, PublishSubject]],
                               right_val: Optional[Any], right_iter: Iterator[Any],
                               is_sync: WindowObservableStates.ConcurrentType) \
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
            right_val_buffer = [[]]

            while True:

                # print('left_val={}, right_val={}'.format(left_val, right_val))

                # right is higher than left
                if self._left_is_lower(left_val, right_val):
                    # print('left is lower, right_val_buffer = {}'.format(right_val_buffer[0]))

                    # send right elements (collected in a list) to inner observer
                    if right_val_buffer[0]:
                        def gen_right():
                            yield from right_val_buffer[0]

                        # ignore acknowledment because observer is completed right away
                        _ = subject.on_next(gen_right)

                        right_val_buffer[0] = []

                    # complete inner observable
                    subject.on_completed()

                    oll_state: WindowObservableStates.OnLeftLowerState = on_left_lower(
                        val_subject_iter=val_subject_iter,
                        last_left_out_ack=last_left_out_ack,
                        right_val=right_val, right_iter=right_iter,
                        is_sync=is_sync)

                    if isinstance(oll_state, WindowObservableStates.SyncMoreLeftOLL):
                        # continue to iterate through right iterable

                        left_val = oll_state.left_val
                        subject = oll_state.subject
                        last_left_out_ack = oll_state.last_left_out_ack
                    elif isinstance(oll_state, WindowObservableStates.SyncNoMoreLeftOLL):
                        # request new left iterable

                        # there are no left values left, request new left value
                        if isinstance(is_sync, WindowObservableStates.SynchronousLeft):
                            # is_empty(left iterable) can only be checked if ack has value
                            return oll_state.left_in_ack
                        elif isinstance(is_sync, WindowObservableStates.SynchronousRight):
                            return oll_state.right_in_ack
                        elif isinstance(is_sync, WindowObservableStates.Asynchronous):
                            return None
                        else:
                            raise Exception('illegal case')

                    elif isinstance(oll_state, WindowObservableStates.AsyncOLL):
                        # crossed asynchronous border

                        # there are no left values left, request new left value
                        if isinstance(is_sync, WindowObservableStates.SynchronousLeft):
                            return oll_state.left_in_ack
                        elif isinstance(is_sync, WindowObservableStates.SynchronousRight):
                            return oll_state.right_in_ack
                        elif isinstance(is_sync, WindowObservableStates.Asynchronous):
                            return None
                        else:
                            raise Exception('illegal case')
                    else:
                        raise Exception('illegal case')

                if self._left_is_higher(left_val, right_val):
                    # update right index
                    right_index_buffer.append((False, right_val))

                else:
                    # update right index
                    right_index_buffer.append((True, right_val))

                    # add to buffer
                    right_val_buffer[0].append(right_val)

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
            if right_val_buffer[0]:
                def gen_right_items_to_inner():
                    yield from right_val_buffer[0]

                inner_ack = subject.on_next(gen_right_items_to_inner)
            else:
                inner_ack = continue_ack

            if right_index_buffer:
                def gen_right_index():
                    yield from right_index_buffer

                right_out_ack = self._index_observer.on_next(gen_right_index)
            else:
                return continue_ack

            if isinstance(is_sync, WindowObservableStates.SynchronousLeft):
                # create a new left in ack
                # because is_sync is SynchronousLeft there has been the possibility that
                # left can be directly back-pressured with continue left

                left_in_ack = Ack()

            elif isinstance(is_sync, WindowObservableStates.SynchronousRight):
                left_in_ack = is_sync.left_in_ack

            elif isinstance(is_sync, WindowObservableStates.Asynchronous):
                left_in_ack = is_sync.left_in_ack

            else:
                raise Exception('illegal case')

            with self._lock:
                if not right_completed[0] and exception[0] is None:
                    new_state = WindowObservableStates.WaitOnRight(left_val=left_val, subject=subject,
                                            val_subject_iter=val_subject_iter,
                                            left_in_ack=left_in_ack,
                                            last_left_out_ack=last_left_out_ack)
                    state[0] = new_state

            if right_completed[0]:
                subject.on_completed()
                for _, subject in val_subject_iter:
                    subject.on_completed()

                self._index_observer.on_completed()
                self._window_observer.on_completed()
                return stop_ack

            elif exception[0] is not None:
                self._index_observer.on_error(exception[0])
                self._window_observer.on_error(exception[0])
                return stop_ack

            # only request a new right, if previous inner and right ack are done
            ack = inner_ack.merge_ack(right_out_ack)

            # right_in_ack gets directly connected to right_out_ack, therefore it
            # does not need to be kept in memory any longer
            if isinstance(is_sync, WindowObservableStates.SynchronousRight):
                return ack
            else:
                right_in_ack = is_sync.right_in_ack
                ack.connect_ack(right_in_ack)

            if isinstance(is_sync, WindowObservableStates.SynchronousLeft):
                return left_in_ack

        def on_next_left(left_elem: Callable[[], Generator]):
            # print('window on left')

            # consume all left elements, and create a Subject for each element
            val_subj_list = [(left, PublishSubject()) for left in left_elem()]

            # # create a generator of element-subject pairs
            def gen_elem_subject_pairs():
                yield from val_subj_list

            val_subject_iter = iter(val_subj_list)
            left_val, subject = next(val_subject_iter)
            last_left_out_ack = self._window_observer.on_next(gen_elem_subject_pairs)

            with self._lock:
                if isinstance(state[0], WindowObservableStates.WaitOnLeft):
                    # iterate over right elements and send them over the publish subject and right observer
                    pass

                elif isinstance(state[0], WindowObservableStates.Completed):
                    return stop_ack

                elif isinstance(state[0], WindowObservableStates.InitialState):
                    # wait until right iterable is received
                    left_in_ack = Ack()
                    new_state = WindowObservableStates.WaitOnRight(left_val=left_val, val_subject_iter=val_subject_iter,
                                            subject=subject,
                                            left_in_ack=left_in_ack,
                                            last_left_out_ack=last_left_out_ack)
                    state[0] = new_state
                    return left_in_ack

                else:
                    raise Exception('illegal state {}'.format(state[0]))

                # save current state to local variable
                current_state: WindowObservableStates.WaitOnLeft = state[0]

                # change state
                state[0] = WindowObservableStates.Transition()

            right_val = current_state.right_val
            right_iter = current_state.right_iter
            right_in_ack = current_state.right_in_ack
            # last_left_out_ack = current_state.last_left_out_ack

            # case 1: state changed to WaitOnRight, returns acknowledgment from left,
            #   and right and inner right connected
            # case 2: state changed to WaitOnLeft, returns acknowledgment from left
            # case 3: state doesn't change, returns None
            is_sync = WindowObservableStates.SynchronousLeft(right_in_ack=right_in_ack)

            left_ack = iterate_over_right(
                left_val=left_val,
                last_left_out_ack=last_left_out_ack,
                subject=subject, val_subject_iter=val_subject_iter,
                right_val=right_val, right_iter=right_iter,
                is_sync=is_sync)

            return left_ack

        def on_next_right(right_elem: Callable[[], Generator]):
            # print('window on right')

            # create the right iterable
            right_iter = right_elem()
            right_val = next(right_iter)

            with self._lock:
                if isinstance(state[0], WindowObservableStates.WaitOnRight):
                    pass
                elif isinstance(state[0], WindowObservableStates.Completed):
                    return stop_ack
                elif isinstance(state[0], WindowObservableStates.InitialState):
                    right_ack = Ack()
                    new_state = WindowObservableStates.WaitOnLeft(right_val=right_val, right_iter=right_iter,
                                           right_in_ack=right_ack)  # , last_left_out_ack=None)
                    state[0] = new_state
                    return right_ack
                else:
                    raise Exception('illegal state {}'.format(state[0]))

                current_state: WindowObservableStates.WaitOnRight = state[0]
                state[0] = WindowObservableStates.Transition()

            left_val = current_state.left_val
            subject = current_state.subject
            val_subject_iter = current_state.val_subject_iter
            left_in_ack = current_state.left_in_ack
            left_out_ack = current_state.last_left_out_ack

            right_ack = iterate_over_right(
                left_val=left_val, val_subject_iter=val_subject_iter,
                last_left_out_ack=left_out_ack,
                subject=subject,
                right_val=right_val, right_iter=right_iter,
                is_sync=WindowObservableStates.SynchronousRight(left_in_ack=left_in_ack, left_out_ack=left_out_ack))

            return right_ack

        def on_error(exc):
            forward_error = False

            with self._lock:
                if exception[0] is not None:
                    return
                elif isinstance(state[0], WindowObservableStates.Transition):
                    pass
                else:
                    forward_error = True
                    state[0] = WindowObservableStates.Completed()

                exception[0] = exc

            if forward_error:
                self._index_observer.on_error(exc)
                self._window_observer.on_error(exc)

        class LeftObserver(Observer):
            def on_next(self, v):
                return on_next_left(v)

            def on_error(self, exc):
                on_error(exc)

            def on_completed(self):
                # print('left completed')
                complete = False

                with source._lock:
                    if left_completed[0]:
                        return
                    elif isinstance(state[0], WindowObservableStates.InitialState) or isinstance(state[0], WindowObservableStates.WaitOnLeft):
                        complete = True
                        state[0] = WindowObservableStates.Completed()
                    else:
                        pass

                    left_completed[0] = True

                if complete:
                    source._window_observer.on_completed()
                    source._index_observer.on_completed()

        class RightObserver(Observer):
            def on_next(self, v):
                return on_next_right(v)

            def on_error(self, exc):
                on_error(exc)

            def on_completed(self):
                complete = False

                with source._lock:
                    if right_completed[0]:
                        return
                    elif isinstance(state[0], WindowObservableStates.InitialState) or isinstance(state[0], WindowObservableStates.WaitOnRight):
                        complete = True
                        state[0] = WindowObservableStates.Completed()
                    else:
                        pass

                    right_completed[0] = True

                if complete:
                    source._window_observer.on_completed()
                    source._index_observer.on_completed()

        left_observer2 = LeftObserver()
        d1 = self._left.observe(left_observer2)

        right_observer2 = RightObserver()
        d2 = self._right.observe(right_observer2)

        return CompositeDisposable(d1, d2)

    def unsafe_subscribe(self, observer: Observer, scheduler: Scheduler, subscribe_scheduler: Scheduler):
        with self._lock:
            self._index_observer = observer

            if isinstance(self._window_observer, EmptyObserver):
                subscribe = True
            else:
                subscribe = False

        if subscribe:
            disposable = self._unsafe_subscribe(scheduler, subscribe_scheduler)
            self._composite_disposable.add(disposable)

        return self._composite_disposable
