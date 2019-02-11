from typing import Callable, Any

from rx import config
from rx.concurrency.schedulerbase import SchedulerBase
from rx.disposables import CompositeDisposable

from rxbackpressurebatched.ack import Stop, Continue, Ack, continue_ack
from rxbackpressurebatched.observable import Observable
from rxbackpressurebatched.observer import Observer
from rxbackpressurebatched.scheduler import Scheduler
from rxbackpressurebatched.subjects.publishsubject import PublishSubject


class ControlledZipObservable(Observable):
    def __init__(self, left: Observable, right: Observable,
                 is_lower: Callable[[Any, Any], bool],
                 is_higher: Callable[[Any, Any], bool],
                 selector: Callable[[Any, Any], Any]):
        """
        :param left:
        :param right:
        :param is_lower: if right is lower than left, request next right
        :param is_higher: if right is higher than left, request next left
        """

        self.left = left
        self.right = right
        self.is_lower = is_lower
        self.is_higher = is_higher
        self.selector = selector

    def unsafe_subscribe(self, observer: Observer, scheduler: Scheduler, subscribe_scheduler: Scheduler):

        left_is_higher = self.is_lower
        left_is_lower = self.is_higher

        right_is_lower = self.is_lower
        right_is_higher = self.is_higher

        lock = config['concurrency'].RLock()

        # right elements is stored in initial state,
        # or if right was higher than last left.

        has_left_elem = [False]
        left_elem = [None]
        left_ack = [None]
        has_right_elem = [False]
        right_elem = [None]
        right_ack = [None]

        has_completed = [False]

        def on_next_left(left):
            # left has been requested because of initial state,
            # or because right is higher than left

            left_elem[0] = left
            left_ack[0] = Ack()

            with lock:
                has_left_elem[0] = True

                if has_right_elem[0]:
                    has_right = True
                else:
                    # race condition: left element is receieved first
                    has_right = False

            if has_right:
                right = right_elem[0]

                if left_is_lower(left, right):
                    # right is higher than left
                    # request new left; don't discard right element

                    # discard left element
                    has_left_elem[0] = False
                    left_elem[0] = None

                    return continue_ack

                if left_is_higher(left, right):
                    # right is lower than left, discard right and request new right
                    # this is possible in initial phase or if is_lower and is_higher are not tight

                    assert right_ack[0] is not None, 'missing acknowledgment'

                    with lock:
                        # avoids completing observer twice

                        # discard right element
                        has_right_elem[0] = False
                        right_elem[0] = None

                        if has_completed[0]:
                            complete_observer = True
                        else:
                            complete_observer = False

                    if complete_observer:
                        # left_observer.on_completed()
                        observer.on_completed()
                        return Stop()

                    right_ack[0].on_next(continue_ack)
                    right_ack[0].on_completed()

                else:
                    # left is equal to right, send right element, request new right

                    with lock:
                        # avoids completing observer twice

                        # discard right element
                        has_right_elem[0] = False
                        right_elem[0] = None

                        if has_completed[0]:
                            complete_observer = True
                        else:
                            complete_observer = False

                    if complete_observer:
                        # left_observer.on_completed()
                        observer.on_completed()
                        return Stop()

                    # send right element
                    ack = observer.on_next(self.selector(left, right))

                    # request new right element
                    ack.connect_ack(right_ack[0])

                    # # request new left element
                    # return ack

            return left_ack[0]

        def on_next_right(right):
            right_elem[0] = right
            right_ack[0] = Ack()

            with lock:
                has_right_elem[0] = True

                if has_left_elem[0]:
                    has_left = True
                else:
                    has_left = False

            if has_left:
                left = left_elem[0]

                if right_is_higher(left, right):
                    # right is higher than left
                    # complete inner observable, discard left and request new left; save right

                    with lock:
                        # avoids completing observer twice

                        # discard left element
                        has_left_elem[0] = False
                        left_elem[0] = None

                        if has_completed[0]:
                            complete_observer = True
                        else:
                            complete_observer = False

                    if complete_observer:
                        observer.on_completed()
                        return Stop()

                    left_ack[0].on_next(continue_ack)
                    left_ack[0].on_completed()

                    return right_ack[0]

                if right_is_lower(left, right):
                    # right is lower than left, discard right and request new right
                    # this is possible in initial phase or if is_lower and is_higher are not tight

                    # discard right element
                    has_right_elem[0] = False
                    right_elem[0] = None

                    return continue_ack
                else:
                    # left is equal to right, send right element, request new right

                    # discard right element
                    has_right_elem[0] = False
                    right_elem[0] = None

                    # send right element
                    ack = observer.on_next(self.selector(left, right))

                    return ack

            else:
                # no left element has been yet received; only possible in initial phase
                return right_ack[0]

        class LeftObserver(Observer):
            def on_next(self, v):
                return on_next_left(v)

            def on_error(self, exc):
                observer.on_error(exc)

            def on_completed(self):
                with lock:
                    if not has_left_elem[0]:
                        # if has_left_elem = False then either
                        # a) on_next_left has set it back to False => complete
                        # b) on_next_right has set it back to False => complete

                        complete_observer = True
                    else:
                        complete_observer = False
                        has_completed[0] = True

                if complete_observer:
                    observer.on_completed()

        class RightObserver(Observer):
            def on_next(self, v):
                return on_next_right(v)

            def on_error(self, exc):
                observer.on_error(exc)

            def on_completed(self):
                with lock:
                    if not has_right_elem[0]:
                        # if has_right_elem = False then either
                        # a) on_next_right has set it back to False => complete
                        # b) on_next_left has set it back to False => complete

                        complete_observer = True
                    else:
                        complete_observer = False
                        has_completed[0] = True

                if complete_observer:
                    observer.on_completed()

        left_observer2 = LeftObserver()
        d1 = self.left.unsafe_subscribe(left_observer2, scheduler, subscribe_scheduler)

        right_observer2 = RightObserver()
        d2 = self.right.unsafe_subscribe(right_observer2, scheduler, subscribe_scheduler)

        return CompositeDisposable(d1, d2)
