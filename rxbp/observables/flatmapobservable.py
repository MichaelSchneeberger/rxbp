import threading
from abc import ABC, abstractmethod
from typing import Callable, Any, List, Optional

from rx.disposable import Disposable, CompositeDisposable
from rxbp.ack.ack import Ack
from rxbp.ack.ackimpl import Continue, continue_ack, Stop, stop_ack
from rxbp.ack.acksubject import AckSubject
from rxbp.ack.single import Single

from rxbp.observable import Observable
from rxbp.observer import Observer
from rxbp.observers.connectableobserver import ConnectableObserver
from rxbp.observesubscription import ObserveSubscription
from rxbp.scheduler import Scheduler
from rxbp.typing import ElementType


class FlatMapObservable(Observable):
    def __init__(self, source: Observable, selector: Callable[[Any], Observable], scheduler: Scheduler,
                 subscribe_scheduler: Scheduler, delay_errors: bool = False):
        self._source = source
        self._selector = selector
        self._scheduler = scheduler
        self._subscribe_scheduler = subscribe_scheduler
        self._delay_errors = delay_errors

        self._lock = threading.RLock()

        self._state = FlatMapObservable.WaitOnOuter(prev_state=None, ack=continue_ack)
        self._conn_observers: Optional[List[ConnectableObserver]] = []
        self._is_inner_active = False

    class State(ABC):
        @abstractmethod
        def get_actual_state(self, nco: int) -> 'FlatMapObservable.State':
            """
            :param nco: number of current inner observer
            """

            return self

    class WaitOnOuter(State):
        def __init__(self, prev_state: Optional['FlatMapObservable.State'], ack: Ack):
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

    class WaitInnerOnCompleteOrOnError(State):
        # outer observer does not complete the output observer (except in WaitOnNextChild); however, it signals
        #   that output observer should be completed if inner observer completes
        def __init__(self, ex, disposable: Disposable = None):
            self.ex = ex
            self.disposable = disposable

        def get_actual_state(self, nco: int):
            if nco == 0:    # number of connectable observables is zero
                return self
            else:
                return FlatMapObservable.Active()

    class Completed(State):
        def get_actual_state(self, nco: int):
            return self

    def observe(self, subscription: ObserveSubscription):
        source = self
        observer = subscription.observer

        composite_disposable = CompositeDisposable()

        def report_invalid_state(state: FlatMapObservable.State, method: str):
            self._scheduler.report_failure(
                Exception('State {} in the ConcatMap.{} implementation is invalid'.format(state, method)))

        class OuterObserver(Observer):
            def __init__(self):
                self.errors = [] if source._delay_errors else None

            def on_next(self, outer_elem: ElementType):

                # materialize received values immediately
                outer_vals = list(outer_elem())

                with source._lock:
                    # blindly set state to Active, verify correctness of current state later
                    prev_state = source._state
                    source._state = FlatMapObservable.Active()

                # previous state should be WaitOnOuter
                if not isinstance(prev_state, FlatMapObservable.WaitOnOuter):
                    # - state should not be Completed, because only outer observer can complete the observable,
                    #   in that case `on_next` should not be called anymore (by rxbp convention)

                    report_invalid_state(prev_state, 'on_next (1)')
                    return stop_ack

                # the ack that might be returned by this `on_next`
                async_upstream_ack = AckSubject()

                def gen_inner_observers():

                    # number of received values
                    vals_len = len(outer_vals)

                    for idx, val in enumerate(outer_vals):

                        # apply selector to get inner observable (per element received)
                        inner_observable = source._selector(val)

                        # create a new `InnerObserver` for each inner observable
                        inner_observer = InnerObserver(downstream_observer=observer, scheduler=source._scheduler,
                                                       async_upstream_ack=async_upstream_ack,
                                                       concat_observer=self, idx=vals_len - idx)

                        # add ConnectableObserver to observe all inner observables simultaneously, and
                        # to get control of activating one after the other
                        conn_observer = ConnectableObserver(inner_observer, scheduler=source._scheduler,
                                                            subscribe_scheduler=source._subscribe_scheduler)
                        conn_subscription = subscription.copy(conn_observer)

                        # observe inner observable
                        disposable = inner_observable.observe(conn_subscription)
                        composite_disposable.add(disposable)

                        yield conn_observer

                # collect connectable observers
                source._conn_observers = list(gen_inner_observers())

                # connect first inner observer
                source._conn_observers[0].connect()

                # possible state transitions
                # - Active -> state did not change
                # - WaitOnOuter -> inner observable completed in the meantime
                # - Completed -> inner observable got an error

                with source._lock:
                    raw_state: FlatMapObservable.State = source._state
                    nco = len(source._conn_observers)

                    # in inner active mode, it is the job of the inner observer to connect the acknowledgment
                    # active power mode requires _is_inner_active == True and state == Active
                    source._is_inner_active = True

                prev_state = raw_state.get_actual_state(nco=nco)

                # inner observable completed during subscription; reset state
                if isinstance(prev_state, FlatMapObservable.WaitOnOuter):

                    # non-concurrent situation
                    source._is_inner_active = False

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
                    source._state = FlatMapObservable.WaitInnerOnCompleteOrOnError(exc)
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
                    source._state = FlatMapObservable.WaitInnerOnCompleteOrOnError(None)
                    nco = len(source._conn_observers)

                prev_state = raw_state.get_actual_state(nco=nco)

                if isinstance(prev_state, FlatMapObservable.WaitOnOuter):
                    # calls to root.on_next and root.on_completed happen in order,
                    # therefore state is not changed concurrently at the WaitOnNextChild
                    observer.on_completed()
                    # observer_completed[0] = True
                    source._state = FlatMapObservable.Completed()

        class InnerObserver(Observer):
            def __init__(self, downstream_observer: Observer, scheduler, async_upstream_ack: AckSubject,
                         concat_observer: Observer, idx: int):
                self.downstream_observer = downstream_observer
                self.scheduler = scheduler
                self.async_upstream_ack = async_upstream_ack
                self.concat_observer = concat_observer
                self.ack = continue_ack
                self.idx = idx

                self.completed = False
                self.exception = None

            def on_next(self, v):

                # on_next, on_completed, on_error are called ordered/non-concurrently
                ack = self.downstream_observer.on_next(v)

                # if ack==Stop, then also update outer observer
                if isinstance(ack, Stop):
                    with source._lock:
                        source._state = FlatMapObservable.Completed()
                elif not isinstance(v, Continue):
                    class ResultSingle(Single):
                        def on_error(self, exc: Exception):
                            raise NotImplementedError

                        def on_next(_, v):
                            if isinstance(v, Stop):
                                with source._lock:
                                    source._state = FlatMapObservable.Completed()
                    ack.subscribe(ResultSingle())

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

            def on_completed(self):
                """ on_next* (on_completed | on_error)?
                """

                last_ack = self.ack

                with source._lock:
                    raw_state = source._state
                    ica = source._is_inner_active
                    source._conn_observers.pop(0)
                    nco = len(source._conn_observers)

                prev_state = raw_state.get_actual_state(nco)

                # possible previous states
                # - Active -> outer on_next call completed before this
                # - WaitOnActiveChild -> outer on_next call completes after this
                # - WaitComplete -> outer.on_complete or outer.on_error was called

                if isinstance(prev_state, FlatMapObservable.Completed):
                    return

                elif isinstance(prev_state, FlatMapObservable.WaitInnerOnCompleteOrOnError):
                    if prev_state.ex is None:
                        observer.on_completed()
                    else:
                        observer.on_error(prev_state.ex)

                    self.async_upstream_ack.on_next(stop_ack)
                    return

                # count is at zero; request new outer
                elif isinstance(prev_state, FlatMapObservable.WaitOnOuter):
                    ack = prev_state.ack

                    with source._lock:
                        prev_state = source._state
                        source._state = FlatMapObservable.WaitOnOuter(prev_state=prev_state, ack=ack)

                    if ica:

                        # non-concurrent situation
                        source._is_inner_active = False

                        last_ack.subscribe(self.async_upstream_ack)

                    else:
                        # if ica is False, last_ack is directly returned by `on_next` method call
                        return

                # connect next child observer
                elif isinstance(prev_state, FlatMapObservable.Active):
                    source._conn_observers[0].connect()

                else:
                    raise Exception('illegal state')

        concat_map_subscription = subscription.copy(OuterObserver())
        d1 = self._source.observe(concat_map_subscription)
        composite_disposable.add(d1)

        return composite_disposable

