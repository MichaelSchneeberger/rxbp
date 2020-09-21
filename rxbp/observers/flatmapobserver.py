import threading
from dataclasses import dataclass
from typing import Callable, Any, List

from rx.disposable import CompositeDisposable

from rxbp.acknowledgement.acksubject import AckSubject
from rxbp.acknowledgement.continueack import continue_ack
from rxbp.acknowledgement.stopack import stop_ack
from rxbp.observable import Observable
from rxbp.observer import Observer
from rxbp.observerinfo import ObserverInfo
from rxbp.observers.connectableobserver import ConnectableObserver
from rxbp.observers.flatmapinnerobserver import FlatMapInnerObserver
from rxbp.scheduler import Scheduler
from rxbp.states.measuredstates.flatmapstates import FlatMapStates
from rxbp.states.rawstates.rawflatmapstates import RawFlatMapStates
from rxbp.typing import ElementType


@dataclass
class FlatMapObserver(Observer):
    observer_info: ObserverInfo
    func: Callable[[Any], Observable]
    scheduler: Scheduler
    subscribe_scheduler: Scheduler
    composite_disposable: CompositeDisposable

    def __post_init__(self):
        self.lock = threading.RLock()
        self.state = [RawFlatMapStates.InitialState()]

    def on_next(self, outer_elem: ElementType):
        if isinstance(outer_elem, list):
            outer_vals = outer_elem
        else:
            try:
                # materialize received values immediately
                outer_vals = list(outer_elem)
            except Exception as exc:
                self.on_error(exc)
                return stop_ack

        if len(outer_vals) == 0:
            return continue_ack

        # the ack that might be returned by this `on_next`
        async_upstream_ack = AckSubject()

        def subscribe_action(_, __):

            conn_observer = None

            for val in reversed(outer_vals[1:]):

                # create a new `InnerObserver` for each inner observable
                inner_observer = FlatMapInnerObserver(
                    observer_info=self.observer_info,
                    next_conn_observer=conn_observer,
                    outer_upstream_ack=async_upstream_ack,
                    state=self.state,
                    lock=self.lock,
                )

                # add ConnectableObserver to observe all inner observables simultaneously, and
                # to get control of activating one after the other
                conn_observer = ConnectableObserver(
                    underlying=inner_observer,
                )

                # apply selector to get inner observable (per element received)
                try:
                    inner_observable = self.func(val)

                    # for mypy to type check correctly
                    assert isinstance(self.observer_info, ObserverInfo)

                    # observe inner observable
                    disposable = inner_observable.observe(self.observer_info.copy(
                        observer=conn_observer,
                    ))
                    self.composite_disposable.add(disposable)
                except Exception as exc:
                    self.on_error(exc)
                    return stop_ack

            # create a new `InnerObserver` for each inner observable
            inner_observer = FlatMapInnerObserver(
                observer_info=self.observer_info,
                next_conn_observer=conn_observer,
                outer_upstream_ack=async_upstream_ack,
                state=self.state,
                lock=self.lock,
            )

            next_state = RawFlatMapStates.Active()

            with self.lock:
                next_state.raw_prev_state = self.state[0]
                self.state[0] = next_state

            meas_state = next_state.raw_prev_state.get_measured_state()

            if isinstance(meas_state, FlatMapStates.Stopped):
                return stop_ack

            # for mypy to type check correctly
            assert isinstance(self.observer_info, ObserverInfo)

            try:
                # apply selector to get inner observable (per element received)
                inner_observable = self.func(outer_vals[0])

                # observe inner observable
                disposable = inner_observable.observe(self.observer_info.copy(
                    observer=inner_observer,
                ))
                self.composite_disposable.add(disposable)
            except Exception as exc:
                self.observer_info.observer.on_error(exc)
                return stop_ack

        if self.subscribe_scheduler.idle:
            disposable = self.subscribe_scheduler.schedule(subscribe_action)
            self.composite_disposable.add(disposable)
        else:
            subscribe_action(None, None)

        return async_upstream_ack

    def on_error(self, exc):
        # next_state = RawFlatMapStates.OnOuterException(exc=exc)
        #
        # with self.lock:
        #     next_state.raw_prev_state = self.state[0]
        #     self.state[0] = next_state
        #
        # meas_prev_state = next_state.raw_prev_state.get_measured_state()
        # meas_state = next_state.get_measured_state()
        #
        # # only if state is WaitOnNextChild, complete observer
        # if isinstance(meas_state, FlatMapStates.Stopped):
        #
        #     if not isinstance(meas_prev_state, FlatMapStates.Stopped):
        #         # calls to root.on_next and root.on_error or root.on_completed happen in order,
        #         # therefore state is not changed concurrently in WaitOnNextChild

        self.observer_info.observer.on_error(exc)

    def on_completed(self):
        next_state = RawFlatMapStates.OnOuterCompleted()

        with self.lock:
            next_state.raw_prev_state = self.state[0]
            self.state[0] = next_state

        meas_prev_state = next_state.raw_prev_state.get_measured_state()
        meas_state = next_state.get_measured_state()

        if isinstance(meas_state, FlatMapStates.Stopped):

            if not isinstance(meas_prev_state, FlatMapStates.Stopped):
                # calls to root.on_next and root.on_completed happen in order,
                # therefore state is not changed concurrently at the WaitOnNextChild
                self.observer_info.observer.on_completed()
