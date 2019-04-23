import threading
from abc import abstractmethod, ABC
from dataclasses import dataclass
from typing import Callable, Any, Iterator, List, Optional, Union

from rx.concurrency import CurrentThreadScheduler, immediate_scheduler
from rx.disposable import Disposable
from rx.disposable import CompositeDisposable, MultipleAssignmentDisposable

from rxbp.ack import Continue, Stop, Ack, stop_ack, continue_ack
from rxbp.observable import Observable
from rxbp.observer import Observer
from rxbp.observers.connectablesubscriber import ConnectableSubscriber
from rxbp.scheduler import Scheduler


class FlatZipObservable(Observable):
    """

    obs1.flat_zip(obs2, lambda v: v[1])


    """

    def __init__(self, left, right, inner_selector: Callable[[Any], Observable],
                 left_selector: Callable[[Any], Any] = None,
                 result_selector: Callable[[Any, Any], Any] = None):
        self.left = left
        self.right = right
        self.selector_left = left_selector or (lambda v: v)
        self.selector_inner = inner_selector
        self.selector = (lambda l, r: (l, r)) if result_selector is None else result_selector

        self.lock = threading.RLock()

    def unsafe_subscribe(self, observer, scheduler, subscribe_scheduler):

        class State(ABC):
            """ The raw state only changes if ilc, iic, irc change

            """

            @abstractmethod
            def get_actual_state(self, ilc: bool, irc: bool) -> 'State':
                ...

        @dataclass
        class WaitLeft(State):
            """ Wait for next left item
            """
            right_val: Any
            right_iter: Iterator
            right_ack: Ack

            def get_actual_state(self, ilc: bool, irc: bool):
                """ current state is
                - Completed if left observable completed
                - WaitLeft otherwise

                :return:
                """
                if ilc:
                    return Completed()
                else:
                    return self

        @dataclass
        class WaitInner(State):
            """ Only inner observable reacts to that state """

            left_val: Any
            left_ack: Ack
            right_val: Any
            right_iter: Iterator
            right_ack: Ack
            output_ack: Ack = None  # needed to request new left or right element

            def get_actual_state(self, ilc: bool, irc: bool):
                return self

        @dataclass
        class WaitRight(State):
            left_val: Any
            left_ack: Ack
            inner_ack: Ack = None
            inner_iter: Iterator = None

            def get_actual_state(self, ilc: bool, irc: bool):
                if irc:
                    return Completed()
                else:
                    return self

        @dataclass
        class WaitLeftRight(State):
            def get_actual_state(self, ilc: bool, irc: bool):
                if ilc or irc:
                    return Completed()
                else:
                    return self

        @dataclass
        class WaitInnerRight(State):
            left_val: Any
            left_ack: Ack

            # inner_iter: Iterator = None
            # inner_ack: Ack = None

            def get_actual_state(self, ilc: bool, irc: bool):
                if irc:
                    return Completed()
                else:
                    return self

        class Completed(State):
            def __init__(self):
                super().__init__()

            def get_actual_state(self, ilc: bool, irc: bool):
                return self

        @dataclass
        class InnerActive(State):
            """ Only one active state can be active, `get_actual_state` returns itself
            """
            # inner_ack: Ack
            right_val: Any
            right_iter: Iterator
            right_ack: Ack

            def get_actual_state(self, ilc: bool, irc: bool):
                return self

        @dataclass
        class InnerCompletedActive(State):
            """ Only one active state can be active, `get_actual_state` returns itself
            """
            right_val: Any
            right_iter: Iterator
            right_ack: Ack
            output_ack: Ack = None  # only available if at least one item has been sent

            def get_actual_state(self, ilc: bool, irc: bool):
                return self

        @dataclass
        class RightActive(State):
            """ Only one active state can be active, `get_actual_state` returns itself
            """
            left_val: Any
            left_ack: Ack
            inner_iter: Iterator
            inner_ack: Ack
            right_val: Any
            right_iter: Iterator
            right_ack: Ack

            def get_actual_state(self, ilc: bool, irc: bool):
                return self

        @dataclass
        class LeftNext(State):
            """ This state is a placeholder for another state, because `get_actual_state` never
            returns itself
            """

            prev_state: Optional[State]
            left_val: Any
            left_ack: Ack

            def get_actual_state(self, ilc: bool, irc: bool):
                prev_state = self.prev_state.get_actual_state(ilc=False, irc=irc)

                if isinstance(prev_state, Completed):
                    return prev_state

                elif isinstance(prev_state, WaitLeftRight):
                    return WaitInnerRight(
                        left_val=self.left_val, left_ack=self.left_ack
                    ).get_actual_state(ilc=ilc, irc=irc)

                elif isinstance(prev_state, WaitLeft):
                    return WaitInner(
                        left_val=self.left_val, left_ack=self.left_ack,
                        right_val=prev_state.right_val, right_iter=prev_state.right_iter,
                        right_ack=prev_state.right_ack
                    ).get_actual_state(ilc=ilc, irc=irc)

                elif isinstance(prev_state, WaitInnerRight):
                    return prev_state

                else:
                    raise Exception('illegal case {}'.format(prev_state))

        @dataclass
        class InnerNext(State):
            """ This state is a placeholder for another state, because `get_actual_state` never
            returns itself
            """

            prev_state: Optional[State]
            inner_iter: Iterator
            inner_ack: Ack  # only used for WaitRight
            idx: int

            def get_actual_state(self, ilc: bool, irc: bool):
                prev_state = self.prev_state.get_actual_state(ilc=ilc, irc=irc)

                if isinstance(prev_state, Completed):
                    return prev_state

                elif isinstance(prev_state, WaitInnerRight):
                    return WaitRight(
                        left_val=prev_state.left_val, left_ack=prev_state.left_ack,
                        inner_ack=self.inner_ack, inner_iter=self.inner_iter
                    ).get_actual_state(ilc=ilc, irc=irc)

                elif isinstance(prev_state, WaitInner):
                    return InnerActive(
                        right_val=prev_state.right_val, right_iter=prev_state.right_iter,
                        right_ack=prev_state.right_ack
                    ).get_actual_state(ilc=ilc, irc=irc)
                    # return WaitInner(
                    #     left_val=prev_state.left_val, left_ack=prev_state.left_ack,
                    #     right_val=prev_state.right_val, right_iter=prev_state.right_iter,
                    #     right_ack=prev_state.right_ack,
                    #
                    # ).get_actual_state(ilc=ilc, irc=irc)

                else:
                    raise Exception('illegal case "{}"'.format(prev_state))

        @dataclass
        class InnerCompleted(State):
            """ This state is a placeholder for another state, because `get_actual_state` never
            returns itself
            """

            prev_state: Optional[State]
            idx: int

            def get_actual_state(self, ilc: bool, irc: bool):
                prev_state = self.prev_state.get_actual_state(ilc=ilc, irc=irc)

                if isinstance(prev_state, Completed):
                    return prev_state

                if isinstance(prev_state, WaitRight):
                    return prev_state

                elif isinstance(prev_state, WaitInnerRight):
                    return WaitLeftRight().get_actual_state(ilc=ilc, irc=irc)

                elif isinstance(prev_state, WaitInner):
                    return InnerCompletedActive(
                        right_val=prev_state.right_val, right_ack=prev_state.right_ack,
                        right_iter=prev_state.right_iter, output_ack=prev_state.output_ack,
                    )
                else:
                    raise Exception('illegal case "{}"'.format(prev_state))

        @dataclass
        class RightNext(State):
            """ This state is a placeholder for another state, because `get_actual_state` never
            returns itself
            """

            prev_state: Optional[State]
            right_val: Any
            right_iter: Iterator
            right_ack: Ack

            def get_actual_state(self, ilc: bool, irc: bool):
                prev_state = self.prev_state.get_actual_state(ilc=ilc, irc=irc)

                if isinstance(prev_state, Completed):
                    return prev_state

                elif isinstance(prev_state, WaitLeftRight):
                    return WaitLeft(
                        right_val=self.right_val, right_iter=self.right_iter,
                        right_ack=self.right_ack
                    ).get_actual_state(ilc=ilc, irc=irc)

                elif isinstance(prev_state, WaitInnerRight):
                    return WaitInner(
                        left_val=prev_state.left_val, left_ack=prev_state.left_ack,
                        right_val=self.right_val, right_iter=self.right_iter,
                        right_ack=self.right_ack,
                    ).get_actual_state(ilc=ilc, irc=irc)

                elif isinstance(prev_state, WaitRight):
                    return RightActive(
                        left_val=prev_state.left_val, left_ack=prev_state.left_ack,
                        inner_ack=prev_state.inner_ack, inner_iter=prev_state.inner_iter,
                        right_val=self.right_val, right_iter=self.right_iter,
                        right_ack=self.right_ack,
                    ).get_actual_state(ilc=ilc, irc=irc)

                else:
                    raise Exception('illegal case')

        is_left_active = [False]
        is_left_completed = [False]
        # is_inner_completed = [False]
        is_right_completed = [False]
        conn_observers: List[List] = [[]]
        state = [WaitLeftRight()]

        inner_disposable = MultipleAssignmentDisposable()
        source = self

        def on_next_left(left_elem):
            # print('flatzip on_next_left')

            left_vals = list(left_elem())
            vals_len = len(left_vals)

            left_ack = Ack()
            new_state = LeftNext(prev_state=None, left_ack=left_ack, left_val=left_vals[0])
            with source.lock:
                prev_state = state[0]
                state[0] = new_state
                state[0].prev_state = prev_state
                is_left_active[0] = True

            # curr_state = new_state.get_actual_state(ilc=False, irc=irc)
            # if isinstance(curr_state, WaitInner):
            #     with source.lock:
            #         state[0] = curr_state

            class ChildObserver(Observer):
                def __init__(self, out: Observer, scheduler, async_upstream_ack: Ack,
                             concat_observer, idx, val):
                    self.out = out
                    self.scheduler = scheduler
                    self.async_upstream_ack = async_upstream_ack
                    self.concat_observer = concat_observer
                    self.ack = Continue()
                    self.idx = idx
                    self.left_val = val

                def on_next(self, inner_elem):

                    # print('on_next inner "{}"'.format(self))

                    inner_ack = Ack()
                    inner_iter = inner_elem()

                    inner_next_state = InnerNext(prev_state=None,  # to be filled synchronously
                                                 inner_ack=inner_ack,
                                                 inner_iter=inner_iter,
                                                 idx=self.idx)
                    with source.lock:
                        prev_state = state[0]
                        state[0] = inner_next_state
                        state[0].prev_state = prev_state
                        ilc = is_left_completed[0]
                        irc = is_right_completed[0]

                    current_state: Union[Completed, WaitRight, InnerActive] = \
                        inner_next_state.get_actual_state(ilc=ilc, irc=irc)

                    if isinstance(current_state, Completed):
                        return stop_ack

                    # wait on right element; don't do anything
                    elif isinstance(current_state, WaitRight):
                        return inner_ack

                    # send inner elements to output observer
                    elif isinstance(current_state, InnerActive):

                        def gen_result():
                            for inner in inner_elem():
                                yield source.selector(self.left_val, current_state.right_val, inner)

                        output_ack = observer.on_next(gen_result)

                        # create WaitInner with new output_ack
                        next_state = WaitInner(
                            left_val=self.left_val, left_ack=left_ack,
                            right_val=current_state.right_val, right_iter=current_state.right_iter,
                            right_ack=current_state.right_ack, output_ack=output_ack)

                        with source.lock:
                            state[0] = next_state  # previous state must be InnerActive

                        return output_ack
                    else:
                        raise Exception('illegal case "{}"'.format(current_state))

                def on_error(self, err):
                    raise NotImplementedError

                def on_completed(self):
                    # print('on_completed_inner "{}"'.format(self))

                    inner_completed_state = InnerCompleted(prev_state=None, idx=self.idx)

                    with source.lock:
                        raw_state = state[0]
                        state[0] = inner_completed_state
                        state[0].prev_state = raw_state
                        ilc = is_left_completed[0]
                        irc = is_right_completed[0]

                        conn_observers[0].pop(0)
                        nco = len(conn_observers[0])

                    prev_state = raw_state.get_actual_state(ilc=ilc, irc=irc)
                    meas_state: Union[Completed, InnerCompletedActive, WaitLeftRight] = \
                        inner_completed_state.get_actual_state(ilc=ilc, irc=irc)

                    if isinstance(meas_state, InnerCompletedActive):

                        # check if there is a right element in iterator
                        try:
                            next_right_val = next(meas_state.right_iter)
                            has_next_right = True
                        except:
                            next_right_val = None
                            has_next_right = False

                        has_next_left = 0 < nco

                        if has_next_right:
                            if has_next_left:
                                next_state = WaitInner(left_ack=left_ack,
                                                       left_val=left_vals,
                                                       right_val=next_right_val,
                                                       right_iter=meas_state.right_iter,
                                                       right_ack=meas_state.right_ack,
                                                       output_ack=meas_state.output_ack
                                                       )
                            else:
                                next_state = WaitLeft(right_val=next_right_val,
                                                      right_iter=meas_state.right_iter,
                                                      right_ack=meas_state.right_ack)
                        else:
                            if has_next_left:
                                next_state = WaitInnerRight(left_ack=left_ack,
                                                            left_val=left_vals)
                            else:
                                next_state = WaitLeftRight()

                        with source.lock:
                            raw_state = state[0]  # previous state must be InnerCompletedActive
                            state[0] = next_state
                            ilc = is_left_completed[0]
                            irc = is_right_completed[0]

                        changed_state = next_state.get_actual_state(ilc=ilc, irc=irc)

                        if isinstance(changed_state, Completed):
                            observer.on_completed()

                            left_ack.on_next(stop_ack)
                            left_ack.on_completed()

                            meas_state.right_ack.on_next(stop_ack)
                            meas_state.right_ack.on_completed()

                        elif isinstance(next_state, WaitInner):
                            def _(ack):
                                if isinstance(ack, Continue):
                                    conn_observers[0][0].connect()
                                else:
                                    raise NotImplementedError

                            if meas_state.output_ack is None:
                                conn_observers[0][0].connect()
                            else:
                                meas_state.output_ack.subscribe(_, on_error=print)
                        elif isinstance(next_state, WaitLeft):
                            meas_state.output_ack.connect_ack(left_ack)

                        elif isinstance(next_state, WaitInnerRight):
                            def _(ack):
                                if isinstance(ack, Continue):
                                    conn_observers[0][0].connect()
                                else:
                                    raise NotImplementedError

                            if meas_state.output_ack is None:
                                conn_observers[0][0].connect()
                            else:
                                meas_state.output_ack.subscribe(_, on_error=print)
                            meas_state.output_ack.connect_ack(meas_state.right_ack)

                        elif isinstance(next_state, WaitLeftRight):

                            if meas_state.output_ack is not None:
                                meas_state.output_ack.connect_ack(left_ack)
                                meas_state.output_ack.connect_ack(meas_state.right_ack)
                            else:
                                continue_ack.connect_ack(left_ack)
                                continue_ack.connect_ack(meas_state.right_ack)

                        else:
                            raise Exception('illegal case "{}"'.format(next_state))

                    # wait for right element
                    elif isinstance(meas_state, WaitLeftRight):
                        pass

                    # wait for right element
                    elif isinstance(meas_state, WaitRight):
                        pass

                    elif isinstance(meas_state, Completed) is not isinstance(prev_state, Completed):

                        # the rule is whoever completes first must complete the observer
                        observer.on_completed()

                    elif isinstance(meas_state, Completed):
                        pass

                    else:
                        raise Exception('illegal case "{}"'.format(meas_state))

            def gen_child_observers():
                for idx, val in enumerate(left_vals):
                    # select child observable from received item
                    child = source.selector_inner(val)

                    # create a new child observer, and subscribe it to child observable
                    child_observer = ChildObserver(observer, scheduler, left_ack, self, vals_len - idx, val)

                    # if 0 < idx:
                    connectable_observer = ConnectableSubscriber(underlying=child_observer,
                                                                 scheduler=immediate_scheduler)
                    disposable = child.unsafe_subscribe(connectable_observer, scheduler, subscribe_scheduler)
                    yield connectable_observer
                    # else:
                    #     disposable = child.unsafe_subscribe(child_observer, scheduler, subscribe_scheduler)
                    #     yield child_observer

            conn_observers[0] = list(gen_child_observers())
            # print(conn_observers[0])
            conn_observers[0][0].connect()

            # left_ack.subscribe(print)

            return left_ack

        def on_next_right(right_elem) -> Ack:
            # print('on_right_inner')

            right_ack = Ack()
            right_iter = right_elem()
            right_val = next(right_iter)

            # right next state has a reference to the previous state, `get_actual_state` will evaluate
            #   the current state as a function of the previous state
            right_next_state = RightNext(prev_state=None,  # to be filled synchronously
                                         right_ack=right_ack, right_iter=right_iter, right_val=right_val)

            with source.lock:
                prev_state = state[0]
                right_next_state.prev_state = prev_state
                state[0] = right_next_state
                ilc = is_left_completed[0]
                irc = is_right_completed[0]
                nco = len(conn_observers[0])

            # to avoid chaining of
            current_state: Union[Completed, WaitLeft, WaitInner, RightActive] = \
                right_next_state.get_actual_state(ilc=ilc, irc=irc)

            # print('current_state {}'.format(current_state))

            if isinstance(current_state, Completed):
                return stop_ack

            # WRL -> WL, wait for left; don't do anything
            elif isinstance(current_state, WaitLeft):
                pass

            # WIR -> WI, wait for inner; don't do anything
            elif isinstance(current_state, WaitInner):
                pass

            # RightActive is a final state, it is safe to override state[0]
            elif isinstance(current_state, RightActive):

                left_ack = current_state.left_ack
                inner_ack = current_state.inner_ack

                def gen_result():
                    for inner_val in current_state.inner_iter:
                        yield source.selector(current_state.left_val, right_val, inner_val)

                output_ack = observer.on_next(gen_result)

                # print('output_ack {}'.format(output_ack))

                try:
                    next_right_val = next(right_iter)
                    has_next_right = True
                except:
                    next_right_val = None
                    has_next_right = False

                has_next_left = 0 < nco

                if has_next_right:
                    if has_next_left:
                        next_state = WaitInner(left_ack=left_ack,
                                               left_val=current_state.left_val,
                                               right_val=next_right_val,
                                               right_iter=right_iter,
                                               right_ack=right_ack
                                               )
                    else:
                        next_state = WaitLeft(right_val=next_right_val, right_iter=right_iter,
                                              right_ack=right_ack)
                else:
                    if has_next_left:
                        next_state = WaitInnerRight(left_ack=left_ack,
                                                    left_val=current_state.left_val)
                    else:
                        next_state = WaitLeftRight()

                # print('next_state {}'.format(next_state))

                with source.lock:
                    prev_state = state[0]
                    state[0] = next_state
                    ilc = is_left_completed[0]
                    irc = is_right_completed[0]

                current_state = next_state.get_actual_state(ilc=ilc, irc=irc)

                # print('current state {}'.format(current_state))

                if isinstance(current_state, Completed):
                    observer.on_completed()

                    left_ack.on_next(stop_ack)
                    left_ack.on_completed()

                    inner_ack.on_next(stop_ack)
                    inner_ack.on_completed()

                    return stop_ack

                elif isinstance(current_state, WaitInner):
                    inner_ack.on_next(continue_ack)
                    inner_ack.on_completed()

                elif isinstance(current_state, WaitLeft):

                    output_ack.connect_ack(left_ack)

                    inner_ack.on_next(stop_ack)
                    inner_ack.on_completed()

                    return right_ack

                elif isinstance(current_state, WaitInnerRight):
                    inner_ack.on_next(continue_ack)
                    inner_ack.on_completed()

                    return output_ack

                elif isinstance(current_state, WaitLeftRight):

                    output_ack.connect_ack(left_ack)
                    # left_ack.on_next(output_ack)
                    # left_ack.on_completed()

                    inner_ack.on_next(stop_ack)
                    inner_ack.on_completed()

                    return output_ack

                # elif isinstance(current_state, WaitInnerRight):
                #     inner_ack.on_next(output_ack)
                #     inner_ack.on_completed()
                #
                #     return output_ack
                #
                # elif isinstance(current_state, WaitInner):
                #
                #     inner_ack.on_next(output_ack)
                #     inner_ack.on_completed()
                #
                #     return right_ack

                else:
                    raise Exception('illegal case {}'.format(current_state))
            else:
                raise Exception('illegal case')

            return right_ack

        def on_completed_left():
            ilc = True

            with source.lock:
                raw_state = state[0]
                prev_ilc = is_left_completed[0]
                irc = is_right_completed[0]
                is_left_completed[0] = ilc

            prev_state = raw_state.get_actual_state(ilc=prev_ilc, irc=irc)
            meas_state = raw_state.get_actual_state(ilc=ilc, irc=irc)

            # complete output observer
            if not isinstance(prev_state, Continue) and isinstance(meas_state, Continue):
                observer.on_completed()

        def on_completed_right():
            irc = True

            with source.lock:
                raw_state = state[0]
                ilc = is_left_completed[0]
                prev_irc = is_right_completed[0]
                is_left_completed[0] = irc

            prev_state = raw_state.get_actual_state(ilc=ilc, irc=prev_irc)
            meas_state = raw_state.get_actual_state(ilc=ilc, irc=irc)

            # complete output observer
            if not isinstance(prev_state, Continue) and isinstance(meas_state, Continue):
                observer.on_completed()

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
