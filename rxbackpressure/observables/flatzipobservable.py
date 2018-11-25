from typing import Callable, Any

from rx import config
from rx.concurrency import CurrentThreadScheduler
from rx.disposables import CompositeDisposable

from rxbackpressure.ack import Continue, Stop, Ack
from rxbackpressure.observable import Observable
from rxbackpressure.observer import Observer


class FlatZipObservable(Observable):
    """

    obs1.flat_zip(obs2, lambda v: v[1])

    """

    def __init__(self, left, right, selector_inner: Callable[[Any], Observable],
                 selector_left: Callable[[Any], Any] = None,
                 selector: Callable[[Any, Any], Any] = None):
        self.left = left
        self.right = right
        self.selector_left = selector_left or (lambda v: v)
        self.selector_inner = selector_inner
        self.selector = (lambda l, r: (l, r)) if selector is None else selector

        self.lock = config['concurrency'].RLock()

    class State:
        pass

    class WaitForLeftOrRight(State):
        pass

    class WaitForRightOrInner(State):
        def __init__(self, left_ack: Ack):
            self.left_ack = left_ack

    class WaitForRight(State):
        def __init__(self, left_ack: Ack, left_elem, inner_left_elem, inner_left_ack: Ack):
            self.left_ack = left_ack
            self.left_elem = left_elem
            self.inner_left_elem = inner_left_elem
            self.inner_left_ack = inner_left_ack

    # class WaitForRightInnerCompleted(State):
    #     def __init__(self, left_ack: Ack, inner_left_elem, inner_left_ack: Ack):
    #         self.left_ack = left_ack
    #         self.inner_left_elem = inner_left_elem
    #         self.inner_left_ack = inner_left_ack

    class WaitForLeft(State):
        def __init__(self, right_elem, right_ack: Ack):
            self.right_elem = right_elem
            self.right_ack = right_ack

    class Active(State):
        def __init__(self, left_ack: Ack, right_elem, right_ack: Ack, upper_ack: Ack):
            self.left_ack = left_ack
            self.right_elem = right_elem
            self.right_ack = right_ack
            self.upper_ack = upper_ack

    class Completed(State):
        pass

    def unsafe_subscribe(self, observer, scheduler, subscribe_scheduler):
        state = [self.WaitForLeftOrRight()]
        current_upper_ack = [None]
        diff_inner_right_completed = [0]
        left_completed = [False]
        right_completed = [False]

        source = self

        def on_next_left(left_elem):
            ack = Ack()

            with self.lock:
                if isinstance(state[0], self.WaitForLeftOrRight):
                    new_state = self.WaitForRightOrInner(left_ack=ack)
                elif isinstance(state[0], self.WaitForLeft):
                    state_: source.WaitForLeft = state[0]
                    new_state = self.Active(left_ack=ack, right_elem=state_.right_elem, right_ack=state_.right_ack,
                                            upper_ack=Continue())
                else:
                    raise NotImplementedError
                state[0] = new_state

            class ChildObserver(Observer):
                def __init__(self, out: Observer, scheduler):
                    self.out = out
                    self.scheduler = scheduler
                    self.is_completed = False

                def on_next(self, inner_left):
                    ack = Ack()
                    has_right_elem = False
                    right_elem = None

                    with source.lock:
                        if isinstance(state[0], source.WaitForRightOrInner):
                            # not much to do
                            state_: source.WaitForRightOrInner = state[0]
                            new_state = source.WaitForRight(inner_left_elem=inner_left, left_ack=state_.left_ack,
                                                            inner_left_ack=ack, left_elem=source.selector_left(left_elem))
                        elif isinstance(state[0], source.Active):
                            # send zipped item to observer
                            state_: source.Active = state[0]
                            has_right_elem = True
                            right_elem = state_.right_elem
                            new_state = source.Active(left_ack=state_.left_ack, right_elem=state_.right_elem,
                                                      right_ack=state_.right_ack, upper_ack=ack)
                        else:
                            raise NotImplementedError
                        state[0] = new_state

                    if has_right_elem:
                        zipped_elem = source.selector(source.selector_left(left_elem), right_elem, inner_left)
                        upper_ack = observer.on_next(zipped_elem)

                        # that's ok, because there is no race-condition here
                        state_: source.Active = state[0]
                        state_.upper_ack = upper_ack

                        # if isinstance(upper_ack, Continue):
                        #     # ack.on_next(upper_ack)
                        #     # ack.on_completed()
                        #     return upper_ack
                        # elif isinstance(upper_ack, Stop):
                        #     raise NotImplementedError
                        # else:
                        #     # upper_ack.observe_on(scheduler).subscribe(ack)
                        return upper_ack
                    else:
                        return ack

                def on_error(self, err):
                    raise NotImplementedError

                def on_completed(self):
                    request_left_right = False
                    request_left = False
                    left_ack = None
                    right_ack = None
                    upper_ack = None
                    is_completed = False

                    with source.lock:
                        if isinstance(state[0], source.WaitForRightOrInner):
                            state_: source.WaitForRight = state[0]
                            # count up number of inner completed (without right completed)
                            diff_inner_right_completed[0] += 1
                            new_state = source.WaitForLeftOrRight()
                            request_left = True
                            left_ack = state_.left_ack
                            upper_ack = Continue()
                        elif isinstance(state[0], source.Active):
                            if right_completed[0]:
                                new_state = source.Completed()
                                is_completed = True
                            else:
                                # request new left and new right
                                state_: source.Active = state[0]
                                request_left_right = True
                                left_ack = state_.left_ack
                                right_ack = state_.right_ack
                                upper_ack = state_.upper_ack
                                new_state = source.WaitForLeftOrRight()
                        else:
                            if self.is_completed:
                                raise Exception('Inner Observable has been completed before')
                            raise NotImplementedError
                        state[0] = new_state

                    self.is_completed = True

                    if is_completed:
                        observer.on_completed()

                    if request_left_right or request_left:
                        if isinstance(upper_ack, Continue) or isinstance(upper_ack, Stop):
                            left_ack.on_next(upper_ack)
                            left_ack.on_completed()
                        elif isinstance(upper_ack, Stop):
                            raise NotImplementedError
                        elif upper_ack is None:
                            raise NotImplementedError
                        else:
                            upper_ack.observe_on(scheduler).subscribe(left_ack)

                    if request_left_right:
                        if isinstance(upper_ack, Continue) or isinstance(upper_ack, Stop):
                            right_ack.on_next(upper_ack)
                            right_ack.on_completed()
                        elif isinstance(upper_ack, Stop):
                            raise NotImplementedError
                        elif upper_ack is None:
                            raise NotImplementedError
                        else:
                            upper_ack.observe_on(scheduler).subscribe(right_ack)

            child = self.selector_inner(left_elem)
            child_observer = ChildObserver(observer, scheduler)

            # todo: save disposable
            disposable = child.subscribe(child_observer, scheduler, CurrentThreadScheduler())

            return ack

        def on_next_right(right):
            ack = Ack()
            upper_ack = None
            has_inner_left_elem = False
            request_left_right = False
            request_right = False

            with self.lock:
                if 0 < diff_inner_right_completed[0]:
                    diff_inner_right_completed[0] -= 1
                    request_right = True
                    upper_ack = Continue()
                    # new_state = self.WaitForLeftOrRight()
                    new_state = state[0]
                elif isinstance(state[0], self.WaitForLeftOrRight):
                    new_state = self.WaitForLeft(right_elem=right, right_ack=ack)
                elif isinstance(state[0], self.WaitForRightOrInner):
                    state_: FlatZipObservable.WaitForRightOrInner = state[0]
                    if diff_inner_right_completed[0] == 0:
                        new_state = self.Active(left_ack=state_.left_ack, right_elem=right, right_ack=ack,
                                                upper_ack=Continue())
                    else:
                        diff_inner_right_completed[0] -= 1
                        new_state = self.WaitForLeftOrRight()
                elif isinstance(state[0], self.WaitForRight):
                    state_: FlatZipObservable.WaitForRight = state[0]
                    has_inner_left_elem = True
                    left_elem = state_.left_elem
                    left_ack = state_.left_ack
                    inner_left_elem = state_.inner_left_elem
                    inner_left_ack = state_.inner_left_ack
                    new_state = state[0]
                else:
                    raise NotImplementedError
                state[0] = new_state

            if has_inner_left_elem:
                zipped_elem = self.selector(left_elem, right, inner_left_elem)
                upper_ack = observer.on_next(zipped_elem)

                request_inner_elem = False
                with self.lock:
                    if 0 < diff_inner_right_completed[0]:
                        # inner left completed, request new left and right
                        new_state = self.WaitForLeftOrRight()
                        request_left_right = True
                    else:
                        # state is the same, change state to active
                        new_state = self.Active(left_ack=left_ack, right_elem=right, right_ack=ack,
                                                 upper_ack=upper_ack)
                        request_inner_elem = True
                    state[0] = new_state

                if request_inner_elem:
                    if isinstance(upper_ack, Continue) or isinstance(upper_ack, Stop):
                        inner_left_ack.on_next(upper_ack)
                        inner_left_ack.on_completed()
                    elif isinstance(upper_ack, Stop):
                        raise NotImplementedError
                    elif upper_ack is None:
                        raise NotImplementedError
                    else:
                        upper_ack.observe_on(scheduler).subscribe(inner_left_ack)

            if request_left_right or request_right:
                if isinstance(upper_ack, Continue):
                    ack.on_next(upper_ack)
                    ack.on_completed()
                elif isinstance(upper_ack, Stop):
                    raise NotImplementedError
                elif upper_ack is None:
                    raise NotImplementedError
                else:
                    upper_ack.observe_on(scheduler).subscribe(ack)

            if request_left_right:
                if isinstance(upper_ack, Continue):
                    left_ack.on_next(upper_ack)
                    left_ack.on_completed()
                elif isinstance(upper_ack, Stop):
                    raise NotImplementedError
                else:
                    upper_ack.observe_on(scheduler).subscribe(left_ack)

            return ack

        def on_completed_left():
            with self.lock:
                left_completed[0] = True

                is_completed = False
                if isinstance(state[0], self.WaitForLeftOrRight) or isinstance(state[0], self.WaitForLeft):
                    new_state = self.Completed()
                    is_completed = True
                else:
                    new_state = state[0]
                state[0] = new_state

            if is_completed:
                return observer.on_completed()

        def on_completed_right():
            with self.lock:
                right_completed[0] = True

                is_completed = False
                if isinstance(state[0], self.WaitForLeftOrRight) or isinstance(state[0], self.WaitForRightOrInner) \
                        or isinstance(state[0], self.WaitForRightOrInner):
                    new_state = self.Completed()
                    is_completed = True
                else:
                    new_state = state[0]
                state[0] = new_state

            if is_completed:
                return observer.on_completed()

        class LeftObserver(Observer):
            def on_next(self, v):
                return on_next_left(v)

            def on_error(self, exc):
                return observer.on_error(exc)

            def on_completed(self):
                return on_completed_left()

        class RightObserver(Observer):
            def on_next(self, v):
                return on_next_right(v)

            def on_error(self, exc):
                return observer.on_error(exc)

            def on_completed(self):
                return on_completed_right()

        left_observer = LeftObserver()
        d1 = self.left.unsafe_subscribe(left_observer, scheduler, subscribe_scheduler)

        right_observer = RightObserver()
        d2 = self.right.unsafe_subscribe(right_observer, scheduler, subscribe_scheduler)

        return CompositeDisposable(d1, d2)