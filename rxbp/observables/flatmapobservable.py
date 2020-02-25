import threading
from typing import Callable, Any, Optional

from rx.disposable import CompositeDisposable

from rxbp.ack.acksubject import AckSubject
from rxbp.ack.continueack import ContinueAck, continue_ack
from rxbp.ack.mixins.ackmixin import AckMixin
from rxbp.ack.single import Single
from rxbp.ack.stopack import StopAck, stop_ack
from rxbp.observable import Observable
from rxbp.observer import Observer
from rxbp.observerinfo import ObserverInfo
from rxbp.observers.connectableobserver import ConnectableObserver
from rxbp.scheduler import Scheduler
from rxbp.states.measuredstates.flatmapstates import FlatMapStates
from rxbp.states.rawstates.rawflatmapstates import RawFlatMapStates
from rxbp.typing import ElementType


class FlatMapObservable(Observable):
    def __init__(
            self,
            source: Observable,
            func: Callable[[Any], Observable],
            scheduler: Scheduler,
            subscribe_scheduler: Scheduler,
            delay_errors: bool = False,
    ):
        self._source = source
        self._func = func
        self._scheduler = scheduler
        self._subscribe_scheduler = subscribe_scheduler
        self._delay_errors = delay_errors

        self.lock = threading.RLock()
        self.state = RawFlatMapStates.InitialState()
        self.composite_disposable = CompositeDisposable()

        self.observer_info: Optional[ObserverInfo] = None

    class InnerObserver(Observer):
        def __init__(
                self,
                outer: 'FlatMapObservable',
                next_conn_observer: Optional[ConnectableObserver],
                outer_upstream_ack: AckSubject,
        ):
            self.outer = outer
            self.next_conn_observer = next_conn_observer
            self.outer_upstream_ack = outer_upstream_ack

        def on_next(self, elem: ElementType):

            # on_next, on_completed, on_error are called ordered/non-concurrently
            ack = self.outer.observer_info.observer.on_next(elem)

            # if ack==Stop, then update state
            if isinstance(ack, StopAck):
                self.outer.state = RawFlatMapStates.Stopped()
                self.outer_upstream_ack.on_next(ack)

            elif not isinstance(ack, ContinueAck):
                class ResultSingle(Single):
                    def on_error(self, exc: Exception):
                        raise NotImplementedError

                    def on_next(_, ack):
                        if isinstance(ack, StopAck):
                            self.outer.state = RawFlatMapStates.Stopped()
                            self.outer_upstream_ack.on_next(ack)
                ack.subscribe(ResultSingle())

            return ack

        def on_error(self, err):
            next_state = RawFlatMapStates.Stopped()

            with self.outer.lock:
                prev_state = self.outer.state
                self.outer.state = next_state

            prev_meas_state = prev_state.get_measured_state()

            if not isinstance(prev_meas_state, FlatMapStates.Stopped):
                self.outer.observer_info.observer.on_error(err)

                # stop outer stream as well
                self.outer_upstream_ack.on_next(stop_ack)

        def on_completed(self):
            """ on_next* (on_completed | on_error)?
            """

            if self.next_conn_observer is None:
                next_state = RawFlatMapStates.WaitOnOuter()

            else:
                next_state = RawFlatMapStates.Active()

            with self.outer.lock:
                next_state.raw_prev_state = self.outer.state
                self.outer.state = next_state

            prev_meas_state = next_state.raw_prev_state.get_measured_state()
            meas_state = next_state.get_measured_state()

            # possible previous states
            # - Active -> outer on_next call completed before this
            # - WaitOnActiveChild -> outer on_next call completes after this
            # - WaitComplete -> outer.on_complete or outer.on_error was called

            # connect next child observer
            if any([
                isinstance(meas_state, FlatMapStates.Active),
                isinstance(meas_state, FlatMapStates.OnOuterCompleted),
            ]):
                self.next_conn_observer.connect()

            elif isinstance(meas_state, FlatMapStates.WaitOnOuter):
                self.outer_upstream_ack.on_next(continue_ack)

            elif isinstance(meas_state, FlatMapStates.Stopped):

                if isinstance(prev_meas_state, FlatMapStates.OnOuterCompleted):
                    self.outer.observer_info.observer.on_completed()
                elif isinstance(prev_meas_state, FlatMapStates.OnOuterException):
                    self.outer.observer_info.observer.on_next(prev_meas_state.exc)
                elif isinstance(prev_meas_state, FlatMapStates.Stopped):
                    pass
                else:
                    raise Exception(f'illegal case "{prev_meas_state}"')

                return

            else:
                raise Exception(f'illegal state "{meas_state}"')

    def _report_invalid_state(self, state: FlatMapStates.State, method: str):
        self._scheduler.report_failure(
            Exception('State {} in the FlatMap.{} implementation is invalid'.format(state, method)))

    def _on_next(self, outer_elem: ElementType):
        if isinstance(outer_elem, list):
            outer_vals = outer_elem
        else:
            try:
                # materialize received values immediately
                outer_vals = list(outer_elem)
            except Exception as exc:
                self._on_error(exc)
                return stop_ack

        # next_state = RawFlatMapStates.Active()
        #
        # with self.lock:
        #     next_state.raw_prev_state = self.state
        #     self.state = next_state
        #
        # meas_state = next_state.raw_prev_state.get_measured_state()
        #
        # if isinstance(meas_state, FlatMapStates.Stopped):
        #     return stop_ack

        # the ack that might be returned by this `on_next`
        async_upstream_ack = AckSubject()

        conn_observer = None

        for val in reversed(outer_vals[1:]):

            # create a new `InnerObserver` for each inner observable
            inner_observer = FlatMapObservable.InnerObserver(
                outer=self,
                next_conn_observer=conn_observer,
                outer_upstream_ack=async_upstream_ack,
            )

            # add ConnectableObserver to observe all inner observables simultaneously, and
            # to get control of activating one after the other
            conn_observer = ConnectableObserver(
                underlying=inner_observer,
                scheduler=self._scheduler,
                subscribe_scheduler=self._subscribe_scheduler,
            )
            conn_subscription = self.observer_info.copy(conn_observer)

            # apply selector to get inner observable (per element received)
            try:
                inner_observable = self._func(val)
            except Exception as exc:
                self._on_error(exc)  # todo: check this
                return stop_ack

            # observe inner observable
            disposable = inner_observable.observe(conn_subscription)
            self.composite_disposable.add(disposable)

        # create a new `InnerObserver` for each inner observable
        inner_observer = FlatMapObservable.InnerObserver(
            outer=self,
            next_conn_observer=conn_observer,
            outer_upstream_ack=async_upstream_ack,
        )
        observer_info = self.observer_info.copy(inner_observer)

        # apply selector to get inner observable (per element received)
        try:
            inner_observable = self._func(outer_vals[0])
        except Exception as exc:
            self._on_error(exc)                       # todo: check this
            return stop_ack

        next_state = RawFlatMapStates.Active()

        with self.lock:
            next_state.raw_prev_state = self.state
            self.state = next_state

        meas_state = next_state.raw_prev_state.get_measured_state()

        if isinstance(meas_state, FlatMapStates.Stopped):
            return stop_ack

        # observe inner observable
        disposable = inner_observable.observe(observer_info)
        self.composite_disposable.add(disposable)

        return async_upstream_ack

    def _on_error(self, exc):
        next_state = RawFlatMapStates.OnOuterException(exc=exc)

        with self.lock:
            next_state.raw_prev_state = self.state
            self.state = next_state

        meas_prev_state = next_state.raw_prev_state.get_measured_state()
        meas_state = next_state.get_measured_state()

        # only if state is WaitOnNextChild, complete observer
        if isinstance(meas_state, FlatMapStates.Stopped):

            if not isinstance(meas_prev_state, FlatMapStates.Stopped):
                # calls to root.on_next and root.on_error or root.on_completed happen in order,
                # therefore state is not changed concurrently in WaitOnNextChild
                self.observer_info.observer.on_error(exc)

    def _on_completed(self):
        next_state = RawFlatMapStates.OnOuterCompleted()

        with self.lock:
            next_state.raw_prev_state = self.state
            self.state = next_state

        meas_prev_state = next_state.raw_prev_state.get_measured_state()
        meas_state = next_state.get_measured_state()

        if isinstance(meas_state, FlatMapStates.Stopped):

            if not isinstance(meas_prev_state, FlatMapStates.Stopped):
                # calls to root.on_next and root.on_completed happen in order,
                # therefore state is not changed concurrently at the WaitOnNextChild
                self.observer_info.observer.on_completed()

    def observe(self, observer_info: ObserverInfo):
        self.observer_info = observer_info

        class FlatMapOuterObserver(Observer):

            def on_next(_, elem: ElementType) -> AckMixin:
                return self._on_next(elem)

            def on_error(_, exc: Exception):
                self._on_error(exc)

            def on_completed(_):
                self._on_completed()

        observer = FlatMapOuterObserver()
        disposable = self._source.observe(observer_info.copy(observer))
        self.composite_disposable.add(disposable)

        return self.composite_disposable
