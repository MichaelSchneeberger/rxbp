from typing import Callable, Any

from rx import config
from rx.concurrency import CurrentThreadScheduler
from rx.disposables import CompositeDisposable, MultipleAssignmentDisposable

from rxbackpressurebatched.ack import Continue, Stop, Ack, stop_ack
from rxbackpressurebatched.observable import Observable
from rxbackpressurebatched.observer import Observer


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
        inner_left_completed = [0]
        right_completed = [False]
        inner_disposable = MultipleAssignmentDisposable()

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
                elif isinstance(state[0], source.Completed):
                    return stop_ack
                else:
                    raise NotImplementedError
                state[0] = new_state

            class ChildObserver(Observer):
                def __init__(self, out: Observer, scheduler):
                    self.out = out
                    self.scheduler = scheduler

                def on_next(self, inner_left):
                    ack = Ack()
                    has_right_elem = False
                    right_elem = None

                    with source.lock:
                        if isinstance(state[0], source.WaitForRightOrInner):
                            # not much to do
                            state_typed: source.WaitForRightOrInner = state[0]
                            state[0] = source.WaitForRight(inner_left_elem=inner_left, left_ack=state_typed.left_ack,
                                                           inner_left_ack=ack,
                                                           left_elem=source.selector_left(left_elem))
                        elif isinstance(state[0], source.Active):
                            # send zipped item to observer
                            state_typed: source.Active = state[0]
                            has_right_elem = True
                            right_elem = state_typed.right_elem
                            state[0] = source.Active(left_ack=state_typed.left_ack, right_elem=state_typed.right_elem,
                                                     right_ack=state_typed.right_ack, upper_ack=ack)
                        elif isinstance(state[0], source.Completed):
                            return stop_ack
                        else:
                            raise NotImplementedError

                    if has_right_elem:
                        # send triple to observer
                        try:
                            zipped_elem = source.selector(source.selector_left(left_elem), right_elem, inner_left)
                        except Exception as exc:
                            print(exc)
                            return self.on_error(exc)
                        upper_ack = observer.on_next(zipped_elem)

                        # race condition with on_completed
                        if isinstance(state[0], source.Active):
                            typed_state: source.Active = state[0]
                            typed_state.upper_ack = upper_ack

                        if isinstance(upper_ack, Stop):
                            with source.lock:
                                state[0] = source.Completed()
                        else:
                            def _(v):
                                if isinstance(v, Stop):
                                    with source.lock:
                                        state[0] = source.Completed()
                            upper_ack.observe_on(scheduler).subscribe(_)
                        return upper_ack
                    else:
                        return ack

                def on_error(self, err):
                    with source.lock:
                        state[0] = source.Completed()
                        observer.on_error(err)

                def on_completed(self):
                    back_pressure_right = False
                    back_pressure_left = False
                    left_ack = None
                    right_ack = None
                    upper_ack = None
                    complete_observer = False

                    with source.lock:
                        if isinstance(state[0], source.Active):
                            # normal complete

                            if right_completed[0]:
                                state[0] = source.Completed()
                                complete_observer = True
                            else:
                                # request new left and new right
                                state_typed: source.Active = state[0]
                                back_pressure_right = True
                                back_pressure_left = True
                                left_ack = state_typed.left_ack
                                right_ack = state_typed.right_ack
                                upper_ack = state_typed.upper_ack
                                state[0] = source.WaitForLeftOrRight()
                        elif isinstance(state[0], source.WaitForRightOrInner):
                            # empty inner observable

                            state_typed: source.WaitForRight = state[0]
                            # count up number of inner completed (without right completed)
                            inner_left_completed[0] += 1
                            state[0] = source.WaitForLeftOrRight()
                            back_pressure_left = True
                            left_ack = state_typed.left_ack
                            upper_ack = Continue()

                    if complete_observer:
                        observer.on_completed()

                    if back_pressure_left or back_pressure_right:
                        def _(v):
                            if isinstance(v, Stop):
                                with source.lock:
                                    state[0] = source.Completed()

                        upper_ack.subscribe(_)

                    if back_pressure_left:
                        # upper_ack should not be Stop
                        if isinstance(upper_ack, Continue):
                            left_ack.on_next(upper_ack)
                            left_ack.on_completed()
                        else:
                            upper_ack.observe_on(scheduler).subscribe(left_ack)

                    if back_pressure_right:
                        if isinstance(upper_ack, Continue):
                            right_ack.on_next(upper_ack)
                            right_ack.on_completed()
                        else:
                            upper_ack.observe_on(scheduler).subscribe(right_ack)

            child = self.selector_inner(left_elem)
            child_observer = ChildObserver(observer, scheduler)

            disposable = child.subscribe(child_observer, scheduler, CurrentThreadScheduler())
            inner_disposable.disposable = disposable

            return ack

        def on_next_right(right):
            ack = Ack()
            upper_ack = None
            has_inner_left_elem = False
            request_left_right = False
            request_right = False

            with self.lock:
                if 0 < inner_left_completed[0]:
                    inner_left_completed[0] -= 1
                    request_right = True
                    upper_ack = Continue()
                    # new_state = self.WaitForLeftOrRight()
                    new_state = state[0]
                elif isinstance(state[0], self.WaitForLeftOrRight):
                    new_state = self.WaitForLeft(right_elem=right, right_ack=ack)
                elif isinstance(state[0], self.WaitForRightOrInner):
                    state_: FlatZipObservable.WaitForRightOrInner = state[0]
                    if inner_left_completed[0] == 0:
                        new_state = self.Active(left_ack=state_.left_ack, right_elem=right, right_ack=ack,
                                                upper_ack=Continue())
                    else:
                        inner_left_completed[0] -= 1
                        new_state = self.WaitForLeftOrRight()
                elif isinstance(state[0], self.WaitForRight):
                    state_: FlatZipObservable.WaitForRight = state[0]
                    has_inner_left_elem = True
                    left_elem = state_.left_elem
                    left_ack = state_.left_ack
                    inner_left_elem = state_.inner_left_elem
                    inner_left_ack = state_.inner_left_ack
                    new_state = state[0]
                elif isinstance(state[0], source.Completed):
                    return stop_ack
                else:
                    raise NotImplementedError
                state[0] = new_state

            if has_inner_left_elem:

                try:
                    zipped_elem = self.selector(left_elem, right, inner_left_elem)
                except Exception as exc:
                    raise NotImplementedError
                upper_ack = observer.on_next(zipped_elem)

                if isinstance(upper_ack, Stop):
                    with self.lock:
                        state[0] = self.Completed
                        return upper_ack

                request_inner_elem = False
                with self.lock:
                    if 0 < inner_left_completed[0]:
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
                    else:
                        upper_ack.observe_on(scheduler).subscribe(inner_left_ack)

                if request_left_right:
                    if isinstance(upper_ack, Continue):
                        left_ack.on_next(upper_ack)
                        left_ack.on_completed()
                    else:
                        upper_ack.observe_on(scheduler).subscribe(left_ack)

            if request_left_right or request_right:
                if isinstance(upper_ack, Continue):
                    ack.on_next(upper_ack)
                    ack.on_completed()
                else:
                    upper_ack.observe_on(scheduler).subscribe(ack)

            return ack

        def on_completed_left():
            with self.lock:
                if isinstance(state[0], self.Completed):
                    return

                if isinstance(state[0], self.WaitForLeftOrRight) or isinstance(state[0], self.WaitForLeft):
                    new_state = self.Completed()
                else:
                    new_state = state[0]
                state[0] = new_state

            if isinstance(state[0], self.Completed):
                return observer.on_completed()

        def on_completed_right():
            with self.lock:
                if isinstance(state[0], self.Completed):
                    return

                if isinstance(state[0], self.WaitForLeftOrRight) or isinstance(state[0], self.WaitForRightOrInner) \
                        or isinstance(state[0], self.WaitForRightOrInner):
                    new_state = self.Completed()
                else:
                    new_state = state[0]
                    right_completed[0] = True
                state[0] = new_state

            if isinstance(state[0], self.Completed):
                return observer.on_completed()

        class LeftObserver(Observer):
            def on_next(self, v):
                return on_next_left(v)

            def on_error(self, exc):
                # todo: adapt this
                raise NotImplementedError

            def on_completed(self):
                return on_completed_left()

        class RightObserver(Observer):
            def on_next(self, v):
                return on_next_right(v)

            def on_error(self, exc):
                raise NotImplementedError

            def on_completed(self):
                return on_completed_right()

        left_observer = LeftObserver()
        d1 = self.left.unsafe_subscribe(left_observer, scheduler, subscribe_scheduler)

        right_observer = RightObserver()
        d2 = self.right.unsafe_subscribe(right_observer, scheduler, subscribe_scheduler)

        return CompositeDisposable(d1, d2, inner_disposable)
