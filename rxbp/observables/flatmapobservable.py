import threading
from typing import Callable, Any, List

from rx.concurrency import immediate_scheduler
from rx.disposable import Disposable

from rxbp.ack import Ack, Continue, Stop, stop_ack
from rxbp.observable import Observable
from rxbp.observer import Observer
from rxbp.observers.connectablesubscriber import ConnectableSubscriber
from rxbp.scheduler import Scheduler


class FlatMapObservable(Observable):
    def __init__(self, source, selector: Callable[[Any], Observable], scheduler: Scheduler, delay_errors=False):
        self.source = source
        self.selector = selector
        self.scheduler = scheduler
        # self.subscribe_scheduler = subscribe_scheduler
        self.delay_errors = delay_errors

    def observe(self, observer: Observer):
        source = self

        class State:
            def get_actual_state(self, nco: int):
                return self

        class WaitOnOuter(State):
            def __init__(self, prev_state, ack):
                self.prev_state = prev_state
                self.ack = ack

            def get_actual_state(self, nco: int):
                if self.prev_state is None or isinstance(self.prev_state, Active):
                    return self
                else:
                    return self.prev_state

        # class WaitOnActiveChild(State):
        #     # this state is necessary in case the inner observable completes before state is set to `Active`; in this
        #     #   case, return the last ack returned by output observer
        #     def __init__(self, outer_iter):
        #         self.outer_iter = outer_iter
        #
        #     def get_actual_state(self, nao: int):
        #         if nao == 0:
        #             return WaitOnNextChild()
        #         else:
        #             return self

        class Active(State):
            def __init__(self, ack = None):
                self.ack = ack

            def get_actual_state(self, nco: int):
                if nco == 0:
                    return WaitOnOuter(prev_state=None, ack=self.ack)
                else:
                    return self

        class WaitOuterOnCompleteOrOnError(State):
            # outer observer does not complete the output observer (except in WaitOnNextChild); however, it signals
            #   that output observer should be completed if inner observer completes
            def __init__(self, ex, disposable: Disposable = None):
                self.ex = ex
                self.disposable = disposable

        class Completed(State):
            pass

        state = [WaitOnOuter(prev_state=None, ack=Continue())]
        conn_observers: List[List] = [None]
        is_child_active = [False]
        lock = threading.RLock()
        source = self

        def report_invalid_state(state: State, method: str):
            self.scheduler.report_failure(
                Exception('State {} in the ConcatMap.{} implementation is invalid'.format(state, method)))

        class ConcatMapObserver(Observer):
            def __init__(self):
                self.errors = [] if source.delay_errors else None

            def on_next(self, outer_elem):
                outer_vals = list(outer_elem())
                vals_len = len(outer_vals)

                with lock:
                    # blindly set state to WaitOnActiveChild, analyse current state later
                    prev_state = state[0]
                    state[0] = Active()

                # nothing happened
                if isinstance(prev_state, WaitOnOuter):
                    pass

                # when should this happen?
                elif isinstance(prev_state, Completed):
                    state[0] = prev_state
                    return stop_ack

                else:
                    report_invalid_state(prev_state, 'on_next (1)')
                    return stop_ack

                # the ack that might be returned by this `on_next`
                async_upstream_ack = Ack()

                def gen_child_observers():
                    for idx, val in enumerate(outer_vals):

                        # select child observable from received item
                        child = source.selector(val)

                        # create a new child observer, and subscribe it to child observable
                        child_observer = ChildObserver(observer, source.scheduler, async_upstream_ack, self, vals_len-idx)

                        if idx == 0:
                            disposable = child.observe(child_observer)
                            yield child_observer
                        else:
                            conn_observer = ConnectableSubscriber(child_observer, scheduler=immediate_scheduler)
                            disposable = child.observe(conn_observer)
                            yield conn_observer

                conn_observers[0] = list(gen_child_observers())

                with lock:
                    # blindly set state to Active, analyse current state later
                    raw_state: State = state[0]
                    is_child_active[0] = True
                    nco = len(conn_observers[0])

                prev_state = raw_state.get_actual_state(nco=nco)

                # possible transitions during child subscription:
                # - WaitOnActiveChild -> nothing happened
                # - WaitOnNextChild -> inner observable completed
                # - Completed -> inner observable got an error

                # inner observable completed during subscription; reset state
                if isinstance(prev_state, WaitOnOuter):
                    return prev_state.ack

                # no state transition happened during child subscription
                elif isinstance(prev_state, Active):
                    return async_upstream_ack

                elif isinstance(prev_state, Completed):
                    return stop_ack

                else:
                    report_invalid_state(prev_state, 'on_next (2)')
                    return stop_ack

            def on_error(self, exc):
                with lock:
                    raw_state = state[0]
                    state[0] = WaitOuterOnCompleteOrOnError(exc)
                    nco = len(conn_observers[0])

                prev_state = raw_state.get_actual_state(nco=nco)

                # only if state is WaitOnNextChild, complete observer
                if isinstance(prev_state, WaitOnOuter):
                    # calls to root.on_next and root.on_error or root.on_completed happen in order,
                    # therefore state is not changed concurrently in WaitOnNextChild
                    observer.on_error(exc)

                    state[0] = Completed()

            def on_completed(self):
                with lock:
                    raw_state = state[0]
                    state[0] = WaitOuterOnCompleteOrOnError(None)
                    nco = len(conn_observers[0])

                prev_state = raw_state.get_actual_state(nco=nco)
                # print('complete outer "{}"'.format(previous_state))

                if isinstance(prev_state, WaitOnOuter):
                    # calls to root.on_next and root.on_completed happen in order,
                    # therefore state is not changed concurrently at the WaitOnNextChild
                    observer.on_completed()
                    # observer_completed[0] = True
                    state[0] = Completed()

        class ChildObserver(Observer):
            def __init__(self, out: Observer, scheduler, async_upstream_ack: Ack,
                         concat_observer, idx):
                self.out = out
                self.scheduler = scheduler
                self.async_upstream_ack = async_upstream_ack
                self.concat_observer = concat_observer
                self.ack = Continue()
                self.idx = idx

                self.completed = False
                self.exception = None

            def on_next(self, v):

                # on_next, on_completed, on_error are called ordered/non-concurrently
                ack = self.out.on_next(v)

                # if ack==Stop, then also update outer observer
                if isinstance(ack, Stop):
                    with lock:
                        state[0] = Completed()
                elif not isinstance(v, Continue):
                    def _(v):
                        if isinstance(v, Stop):
                            with lock:
                                state[0] = Completed()
                    ack.subscribe(_)

                self.ack = ack
                return ack

            def on_error(self, err):
                with lock:
                    # previous_completed = observer_completed[0]
                    # observer_completed[0] = True
                    previous_state = state[0]
                    state[0] = Completed()

                if not isinstance(previous_state, Completed):
                    observer.on_error(err)

                    self.async_upstream_ack.on_next(stop_ack)
                    self.async_upstream_ack.on_completed()

            def on_completed(self):
                """ on_next* (on_completed | on_error)?

                :return:
                """

                last_ack = self.ack
                # next_raw_state = Active()

                with lock:
                    raw_state = state[0]
                    # state[0] = next_raw_state
                    ica = is_child_active[0]
                    conn_observers[0].pop(0)
                    nco = len(conn_observers[0])

                prev_state = raw_state.get_actual_state(nco)

                # possible previous states
                # - Active -> outer on_next call completed before this
                # - WaitOnActiveChild -> outer on_next call completes after this
                # - WaitComplete -> outer.on_complete or outer.on_error was called

                if isinstance(prev_state, Completed):
                    return

                elif isinstance(prev_state, WaitOuterOnCompleteOrOnError):
                    if prev_state.ex is None:
                        observer.on_completed()
                    else:
                        observer.on_error(prev_state.ex)

                    self.async_upstream_ack.on_next(stop_ack)
                    self.async_upstream_ack.on_completed()
                    return

                # count is at zero; request new outer
                elif isinstance(prev_state, WaitOnOuter):
                    ack = prev_state.ack

                    with lock:
                        prev_state = state[0]
                        state[0] = WaitOnOuter(prev_state=prev_state, ack=ack)

                    if ica:
                        last_ack.connect_ack(self.async_upstream_ack)

                    else:
                        return

                # check if the first element of next inner has already been received
                elif isinstance(prev_state, Active):
                    conn_observers[0][0].connect()

                else:
                    raise Exception('illegal state')

        concat_map_observer = ConcatMapObserver()
        return self.source.observe(concat_map_observer)

