from typing import Callable, Any

from rx import config
from rx.concurrency.schedulerbase import SchedulerBase
from rx.disposables import CompositeDisposable

from rxbackpressure.ack import Stop, Continue, Ack
from rxbackpressure.observable import Observable
from rxbackpressure.observer import Observer
from rxbackpressure.subjects.publishsubject import PublishSubject


def window(left: Observable, right: Observable,
                 is_lower: Callable[[Any, Any], bool],
                 is_higher: Callable[[Any, Any], bool]):
    """
    :param left:
    :param right:
    :param is_lower: if right is lower than left, request next right
    :param is_higher: if right is higher than left, request next left
    """

    left_is_higher = is_lower
    left_is_lower = is_higher

    lock = config['concurrency'].RLock()

    def unsafe_subscribe(left_observer: Observer, right_observer: Observer, scheduler: SchedulerBase,
                         subscribe_scheduler: SchedulerBase):

        # right elements is stored in initial state,
        # or if right was higher than last left.

        has_left_elem = [False]
        left_elem = [None]
        left_ack = [None]
        has_right_elem = [False]
        right_elem = [None]
        right_ack = [None]

        # request next left only after observer requests new element
        outer_ack = [None]
        publish_subject = [None]

        has_completed = [False]

        def on_next_left(left):
            # left has been requested because of initial state,
            # or because right is higher than left

            left_elem[0] = left
            left_ack[0] = Ack()
            publish_subject[0] = PublishSubject()

            # send next inner observable; subscribe needs to happen immediately
            outer_ack[0] = left_observer.on_next((left, publish_subject[0]))  # todo: make this private

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
                    # send (empty) inner observable and request new left; don't discard right element
                    # inner observable is empty, because left has just been received

                    # discard left element
                    has_left_elem[0] = False
                    left_elem[0] = None

                    # complete (empty) inner observable
                    publish_subject[0].on_completed()

                    # continue next left element after outer acknowledgment
                    return outer_ack[0]
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
                        left_observer.on_completed()
                        right_observer.on_completed()
                        return Stop()

                    ack = right_observer.on_next((False, right))

                    # request new right
                    ack.connect_ack(next_ack=right_ack[0])

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
                        left_observer.on_completed()
                        right_observer.on_completed()
                        return Stop()

                    # send right element
                    ack = publish_subject[0].on_next(right)
                    ack2 = right_observer.on_next((True, right))

                    # request new right element
                    ack.connect_ack_2(ack2=ack2, out_ack=right_ack[0])

            return_ack = left_ack[0].merge_ack(outer_ack[0])

            # return left_ack[0]
            return return_ack

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

                if is_higher(left, right):
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
                        left_observer.on_completed()
                        right_observer.on_completed()
                        return Stop()

                    # complete inner observable
                    publish_subject[0].on_completed()

                    left_ack[0].on_next(Continue())
                    left_ack[0].on_completed()

                    return right_ack[0]

                if is_lower(left, right):
                    # right is lower than left, discard right and request new right
                    # this is possible in initial phase or if is_lower and is_higher are not tight

                    ack = right_observer.on_next((False, right))

                    # discard right element
                    has_right_elem[0] = False
                    right_elem[0] = None

                    return ack
                else:
                    # left is equal to right, send right element, request new right

                    # discard right element
                    has_right_elem[0] = False
                    right_elem[0] = None

                    # send right element
                    ack = publish_subject[0].on_next(right)
                    ack2 = right_observer.on_next((True, right))

                    return ack.merge_ack(ack2)

            else:
                # no left element has been yet received; only possible in initial phase
                return right_ack[0]

        class LeftObserver(Observer):
            def on_next(self, v):
                return on_next_left(v)

            def on_error(self, exc):
                right_observer.on_error(exc)
                return left_observer.on_error(exc)

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
                    left_observer.on_completed()
                    right_observer.on_completed()

        class RightObserver(Observer):
            def on_next(self, v):
                return on_next_right(v)

            def on_error(self, exc):
                right_observer.on_error(exc)
                return left_observer.on_error(exc)

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
                    left_observer.on_completed()
                    right_observer.on_completed()

        left_observer2 = LeftObserver()
        d1 = left.unsafe_subscribe(left_observer2, scheduler, subscribe_scheduler)

        right_observer2 = RightObserver()
        d2 = right.unsafe_subscribe(right_observer2, scheduler, subscribe_scheduler)

        return CompositeDisposable(d1, d2)

    left_observer = [None]
    right_observer = [None]

    class LeftObservable(Observable):
        def unsafe_subscribe(self, observer, scheduler, s):
            if right_observer[0]:
                unsafe_subscribe(observer, right_observer[0], scheduler, s)
            else:
                left_observer[0] = observer

    o1 = LeftObservable()

    class RightObservable(Observable):
        def unsafe_subscribe(self, observer, scheduler, s):
            if left_observer[0]:
                unsafe_subscribe(left_observer[0], observer, scheduler, s)
            else:
                right_observer[0] = observer

    o2 = RightObservable()

    return o1, o2