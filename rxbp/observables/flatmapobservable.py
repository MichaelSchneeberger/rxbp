import sys
import threading
from abc import ABC, abstractmethod
from typing import Callable, Any, List, Optional

from rx.disposable import Disposable, CompositeDisposable
from rxbp.ack.ack import Ack
from rxbp.ack.ackbase import AckBase
from rxbp.ack.ackimpl import Continue, continue_ack, Stop, stop_ack
from rxbp.ack.acksubject import AckSubject
from rxbp.ack.single import Single

from rxbp.observable import Observable
from rxbp.observer import Observer
from rxbp.observers.connectableobserver import ConnectableObserver
from rxbp.observerinfo import ObserverInfo
from rxbp.scheduler import Scheduler
from rxbp.typing import ValueType, ElementType


class FlatMapObservable(Observable):
    def __init__(
            self,
            source: Observable,
            selector: Callable[[Any], Observable],
            scheduler: Scheduler,
            subscribe_scheduler: Scheduler,
            delay_errors: bool = False,
    ):
        self._source = source
        self._selector = selector
        self._scheduler = scheduler
        self._subscribe_scheduler = subscribe_scheduler
        self._delay_errors = delay_errors

        self.lock = threading.RLock()
        self.state = FlatMapObservable.InitialState()
        self.composite_disposable = CompositeDisposable()

        self.observer_info = None

    class State(ABC):
        @abstractmethod
        def get_measured_state(self) -> 'FlatMapObservable.State':
            ...

    class InitialState(State):
        def get_measured_state(self) -> 'FlatMapObservable.State':
            return self

    class WaitOnOuter(State):
        def __init__(self):
            self.prev_state: Optional['FlatMapObservable.State'] = None

        def get_measured_state(self):
            meas_prev_state = self.prev_state.get_measured_state()

            if any([
                isinstance(meas_prev_state, FlatMapObservable.Stopped),
                isinstance(meas_prev_state, FlatMapObservable.OnOuterException),
                isinstance(meas_prev_state, FlatMapObservable.OnOuterCompleted)
            ]):
                return FlatMapObservable.Stopped()

            else:
                return self

    class Active(State):
        """ Outer value received, inner observable possibly already subscribed
        """

        def __init__(self):
            self.prev_state: Optional['FlatMapObservable.State'] = None

        def get_measured_state(self):
            meas_prev_state = self.prev_state.get_measured_state()

            if any([
                isinstance(meas_prev_state, FlatMapObservable.Stopped),
                isinstance(meas_prev_state, FlatMapObservable.OnOuterException),
            ]):
                return FlatMapObservable.Stopped()

            elif isinstance(meas_prev_state, FlatMapObservable.OnOuterCompleted):
                return meas_prev_state

            else:
                return self

    class OnOuterCompleted(State):
        # outer observer does not complete the output observer (except in WaitOnNextChild); however, it signals
        #   that output observer should be completed if inner observer completes
        def __init__(self):
            self.prev_state: Optional['FlatMapObservable.State'] = None

        def get_measured_state(self):
            meas_prev_state = self.prev_state.get_measured_state()

            if isinstance(meas_prev_state, FlatMapObservable.Stopped):
                return meas_prev_state
            elif isinstance(meas_prev_state, FlatMapObservable.WaitOnOuter):
                return FlatMapObservable.Stopped()
            else:
                return self

    class OnOuterException(State):
        def __init__(self, exc: Exception):
            self.exc = exc

            self.prev_state: Optional['FlatMapObservable.State'] = None

        def get_measured_state(self):
            meas_prev_state = self.prev_state.get_measured_state()

            if isinstance(meas_prev_state, FlatMapObservable.Stopped):
                return meas_prev_state
            elif isinstance(meas_prev_state, FlatMapObservable.WaitOnOuter):
                return FlatMapObservable.Stopped()
            else:
                return self

    class Stopped(State):
        """ either inner observable completed or raise exception
        """
        def get_measured_state(self):
            return self

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
            # print(f'FlatMap.on_next({elem})')

            # on_next, on_completed, on_error are called ordered/non-concurrently
            ack = self.outer.observer_info.observer.on_next(elem)

            # if ack==Stop, then update state
            if isinstance(ack, Stop):
                self.outer.state = FlatMapObservable.Stopped()
                self.outer_upstream_ack.on_next(ack)

            elif not isinstance(ack, Continue):
                class ResultSingle(Single):
                    def on_error(self, exc: Exception):
                        raise NotImplementedError

                    def on_next(_, ack):
                        if isinstance(ack, Stop):
                            self.outer.state = FlatMapObservable.Stopped()
                            self.outer_upstream_ack.on_next(ack)
                ack.subscribe(ResultSingle())

            return ack

        def on_error(self, err):
            next_state = FlatMapObservable.Stopped()

            with self.outer.lock:
                prev_state = self.outer.state
                self.outer.state = next_state

            prev_meas_state = prev_state.get_measured_state()

            if not isinstance(prev_meas_state, FlatMapObservable.Stopped):
                self.outer.observer_info.observer.on_error(err)

                # stop outer stream as well
                self.outer_upstream_ack.on_next(stop_ack)

        def on_completed(self):
            """ on_next* (on_completed | on_error)?
            """

            # last_ack = self.last_ack

            if self.next_conn_observer is None:
                next_state = FlatMapObservable.WaitOnOuter()

            else:
                next_state = FlatMapObservable.Active()

            with self.outer.lock:
                prev_state = self.outer.state
                next_state.prev_state = prev_state
                self.outer.state = next_state

            prev_meas_state = prev_state.get_measured_state()
            meas_state = next_state.get_measured_state()

            # possible previous states
            # - Active -> outer on_next call completed before this
            # - WaitOnActiveChild -> outer on_next call completes after this
            # - WaitComplete -> outer.on_complete or outer.on_error was called

            # connect next child observer
            if any([
                isinstance(meas_state, FlatMapObservable.Active),
                isinstance(meas_state, FlatMapObservable.OnOuterCompleted),
            ]):
                self.next_conn_observer.connect()

            elif isinstance(meas_state, FlatMapObservable.WaitOnOuter):
                self.outer_upstream_ack.on_next(continue_ack)

            elif isinstance(meas_state, FlatMapObservable.Stopped):

                if isinstance(prev_meas_state, FlatMapObservable.OnOuterCompleted):
                    self.outer.observer_info.observer.on_completed()
                elif isinstance(prev_meas_state, FlatMapObservable.OnOuterException):
                    self.outer.observer_info.observer.on_next(prev_meas_state.exc)
                elif isinstance(prev_meas_state, FlatMapObservable.Stopped):
                    pass
                else:
                    raise Exception(f'illegal case "{prev_meas_state}"')

                return

            else:
                raise Exception(f'illegal state "{meas_state}"')

    def _report_invalid_state(self, state: 'FlatMapObservable.State', method: str):
        self._scheduler.report_failure(
            Exception('State {} in the ConcatMap.{} implementation is invalid'.format(state, method)))

    def _on_next(self, outer_elem: ElementType):

        if isinstance(outer_elem, list):
            outer_vals = outer_elem
        else:
            try:
                # materialize received values immediately
                outer_vals = list(outer_elem)
            except:
                exc = sys.exc_info()
                self._on_error(exc)
                return stop_ack

        next_state = FlatMapObservable.Active()

        with self.lock:
            prev_state = self.state
            next_state.prev_state = prev_state
            self.state = next_state

        meas_state = next_state.get_measured_state()

        if isinstance(meas_state, FlatMapObservable.Stopped):
            return stop_ack

        # previous state should be WaitOnOuter
        if not (isinstance(prev_state, FlatMapObservable.InitialState) or isinstance(prev_state, FlatMapObservable.WaitOnOuter)):
            # - state should not be Completed, because only outer observer can complete the observable,
            #   in that case `on_next` should not be called anymore (by rxbp convention)

            self._report_invalid_state(prev_state, 'on_next (1)')
            return stop_ack

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
            inner_observable = self._selector(val)

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
        inner_observable = self._selector(outer_vals[0])

        # observe inner observable
        disposable = inner_observable.observe(observer_info)
        self.composite_disposable.add(disposable)

        return async_upstream_ack

    def _on_error(self, exc):
        next_state = FlatMapObservable.OnOuterException(exc=exc)

        with self.lock:
            prev_state = self.state
            next_state.prev_state = prev_state
            self.state = next_state

        meas_prev_state = prev_state.get_measured_state()
        meas_state = next_state.get_measured_state()

        # only if state is WaitOnNextChild, complete observer
        if isinstance(meas_state, FlatMapObservable.Stopped):

            if not isinstance(meas_prev_state, FlatMapObservable.Stopped):
                # calls to root.on_next and root.on_error or root.on_completed happen in order,
                # therefore state is not changed concurrently in WaitOnNextChild
                self.observer_info.observer.on_error(exc)

    def _on_completed(self):
        next_state = FlatMapObservable.OnOuterCompleted()

        with self.lock:
            prev_state = self.state
            next_state.prev_state = prev_state
            self.state = next_state

        meas_prev_state = prev_state.get_measured_state()
        meas_state = next_state.get_measured_state()

        if isinstance(meas_state, FlatMapObservable.Stopped):

            if not isinstance(meas_prev_state, FlatMapObservable.Stopped):
                # calls to root.on_next and root.on_completed happen in order,
                # therefore state is not changed concurrently at the WaitOnNextChild
                self.observer_info.observer.on_completed()

    def observe(self, observer_info: ObserverInfo):
        self.observer_info = observer_info

        class FlatMapOuterObserver(Observer):

            def on_next(_, elem: ElementType) -> AckBase:
                return self._on_next(elem)

            def on_error(_, exc: Exception):
                self._on_error(exc)

            def on_completed(_):
                self._on_completed()

        observer = FlatMapOuterObserver()
        disposable = self._source.observe(observer_info.copy(observer))
        self.composite_disposable.add(disposable)

        return self.composite_disposable
