import threading
from typing import Callable, Any, List, Optional

from rx.disposable import Disposable

from rxbp.ack import Ack, Continue, Stop, stop_ack, continue_ack
from rxbp.observable import Observable
from rxbp.observer import Observer
from rxbp.observers.connectablesubscriber import ConnectableSubscriber
from rxbp.scheduler import Scheduler
from rxbp.schedulers.trampolinescheduler import TrampolineScheduler


class FlatMapObservable(Observable):
    def __init__(self, source: Observable, selector: Callable[[Any], Observable], scheduler: Scheduler, delay_errors = False):
        self._source = source
        self._selector = selector
        self._scheduler = scheduler
        self._delay_errors = delay_errors

        self._lock = threading.RLock()
        self._state = FlatMapObservable.WaitOnOuter(prev_state=None, ack=Continue())
        self._conn_observers: Optional[List[List]] = None
        self._is_child_active = False

    class State:
        def get_actual_state(self, nco: int):
            return self

    class WaitOnOuter(State):
        def __init__(self, prev_state, ack):
            self.prev_state = prev_state
            self.ack = ack

        def get_actual_state(self, nco: int):
            if self.prev_state is None or isinstance(self.prev_state, FlatMapObservable.Active):
                return self
            else:
                return self.prev_state

    class Active(State):
        def get_actual_state(self, nco: int):
            if nco == 0:    # number of connectable observables is zero
                return FlatMapObservable.WaitOnOuter(prev_state=None, ack=continue_ack)
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

    def observe(self, observer: Observer):
        source = self

        def report_invalid_state(state: FlatMapObservable.State, method: str):
            self._scheduler.report_failure(
                Exception('State {} in the ConcatMap.{} implementation is invalid'.format(state, method)))

        class ConcatMapObserver(Observer):
            def __init__(self):
                self.errors = [] if source._delay_errors else None

            def on_next(self, outer_elem):
                outer_vals = list(outer_elem())
                vals_len = len(outer_vals)

                with source._lock:
                    # blindly set state to WaitOnActiveChild, analyse current state later
                    prev_state = source._state
                    source._state = FlatMapObservable.Active()

                # nothing happened
                if isinstance(prev_state, FlatMapObservable.WaitOnOuter):
                    pass

                # when should this happen?
                elif isinstance(prev_state, FlatMapObservable.Completed):
                    source._state = prev_state
                    return stop_ack

                else:
                    report_invalid_state(prev_state, 'on_next (1)')
                    return stop_ack

                # the ack that might be returned by this `on_next`
                async_upstream_ack = Ack()

                def gen_child_observers():
                    for idx, val in enumerate(outer_vals):

                        # select child observable from received item
                        child = source._selector(val)

                        # create a new child observer, and subscribe it to child observable
                        child_observer = ChildObserver(observer, source._scheduler, async_upstream_ack, self, vals_len - idx)

                        # if idx == 0:
                        #     disposable = child.observe(child_observer)
                        #     yield child_observer
                        # else:

                        scheduler = TrampolineScheduler()
                        conn_observer = ConnectableSubscriber(child_observer, scheduler=scheduler, subscribe_scheduler=scheduler)
                        disposable = child.observe(conn_observer)
                        yield conn_observer

                source._conn_observers = list(gen_child_observers())

                # connect first Observer
                source._conn_observers[0].connect()

                with source._lock:
                    # blindly set state to Active, analyse current state later
                    raw_state: FlatMapObservable.State = source._state
                    source._is_child_active = True
                    nco = len(source._conn_observers)

                prev_state = raw_state.get_actual_state(nco=nco)

                # possible transitions during child subscription:
                # - WaitOnActiveChild -> nothing happened
                # - WaitOnNextChild -> inner observable completed
                # - Completed -> inner observable got an error

                # inner observable completed during subscription; reset state
                if isinstance(prev_state, FlatMapObservable.WaitOnOuter):
                    return prev_state.ack

                # no state transition happened during child subscription
                elif isinstance(prev_state, FlatMapObservable.Active):
                    return async_upstream_ack

                elif isinstance(prev_state, FlatMapObservable.Completed):
                    return stop_ack

                else:
                    report_invalid_state(prev_state, 'on_next (2)')
                    return stop_ack

            def on_error(self, exc):
                with source._lock:
                    raw_state = source._state
                    source._state = FlatMapObservable.WaitOuterOnCompleteOrOnError(exc)
                    nco = len(source._conn_observers)

                prev_state = raw_state.get_actual_state(nco=nco)

                # only if state is WaitOnNextChild, complete observer
                if isinstance(prev_state, FlatMapObservable.WaitOnOuter):
                    # calls to root.on_next and root.on_error or root.on_completed happen in order,
                    # therefore state is not changed concurrently in WaitOnNextChild
                    observer.on_error(exc)

                    source._state = FlatMapObservable.Completed()

            def on_completed(self):
                with source._lock:
                    raw_state = source._state
                    source._state = FlatMapObservable.WaitOuterOnCompleteOrOnError(None)
                    nco = len(source._conn_observers)

                prev_state = raw_state.get_actual_state(nco=nco)
                # print('complete outer "{}"'.format(previous_state))

                if isinstance(prev_state, FlatMapObservable.WaitOnOuter):
                    # calls to root.on_next and root.on_completed happen in order,
                    # therefore state is not changed concurrently at the WaitOnNextChild
                    observer.on_completed()
                    # observer_completed[0] = True
                    source._state = FlatMapObservable.Completed()

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
                    with source._lock:
                        source._state = FlatMapObservable.Completed()
                elif not isinstance(v, Continue):
                    def _(v):
                        if isinstance(v, Stop):
                            with source._lock:
                                source._state = FlatMapObservable.Completed()
                    ack.subscribe(_)

                self.ack = ack
                return ack

            def on_error(self, err):
                with source._lock:
                    # previous_completed = observer_completed[0]
                    # observer_completed[0] = True
                    previous_state = source._state
                    source._state = FlatMapObservable.Completed()

                if not isinstance(previous_state, FlatMapObservable.Completed):
                    observer.on_error(err)

                    self.async_upstream_ack.on_next(stop_ack)
                    self.async_upstream_ack.on_completed()

            def on_completed(self):
                """ on_next* (on_completed | on_error)?

                :return:
                """

                last_ack = self.ack
                # next_raw_state = Active()

                with source._lock:
                    raw_state = source._state
                    # source.state = next_raw_state
                    ica = source._is_child_active
                    source._conn_observers.pop(0)
                    nco = len(source._conn_observers)

                prev_state = raw_state.get_actual_state(nco)

                # possible previous states
                # - Active -> outer on_next call completed before this
                # - WaitOnActiveChild -> outer on_next call completes after this
                # - WaitComplete -> outer.on_complete or outer.on_error was called

                if isinstance(prev_state, FlatMapObservable.Completed):
                    return

                elif isinstance(prev_state, FlatMapObservable.WaitOuterOnCompleteOrOnError):
                    if prev_state.ex is None:
                        observer.on_completed()
                    else:
                        observer.on_error(prev_state.ex)

                    self.async_upstream_ack.on_next(stop_ack)
                    self.async_upstream_ack.on_completed()
                    return

                # count is at zero; request new outer
                elif isinstance(prev_state, FlatMapObservable.WaitOnOuter):
                    ack = prev_state.ack

                    with source._lock:
                        prev_state = source._state
                        source._state = FlatMapObservable.WaitOnOuter(prev_state=prev_state, ack=ack)

                    if ica:
                        last_ack.connect_ack(self.async_upstream_ack)

                    else:
                        return

                # check if the first element of next inner has already been received
                elif isinstance(prev_state, FlatMapObservable.Active):
                    source._conn_observers[0].connect()

                else:
                    raise Exception('illegal state')

        concat_map_observer = ConcatMapObserver()
        return self._source.observe(concat_map_observer)

