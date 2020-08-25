import threading
from dataclasses import dataclass
from typing import Callable, Any, List, Optional

from rx.disposable import CompositeDisposable

from rxbp.acknowledgement.continueack import continue_ack
from rxbp.acknowledgement.ack import Ack
from rxbp.acknowledgement.single import Single
from rxbp.observable import Observable
from rxbp.observer import Observer
from rxbp.observerinfo import ObserverInfo
from rxbp.observers.connectableobserver import ConnectableObserver
from rxbp.scheduler import Scheduler
from rxbp.typing import ElementType


@dataclass
class FlatConcatNoBackpressureObserver(Observer):
    observer: Observer
    selector: Callable[[Any], Observable]
    scheduler: Scheduler
    subscribe_scheduler: Scheduler
    observer_info: ObserverInfo
    composite_disposable: CompositeDisposable

    def __post_init__(self):
        self.lock = threading.RLock()
        # self.conn_observers: List[ConnectableObserver] = []

        self.inner_observer = self.InnerObserver(
            observer=self.observer,
            last_ack=None,
            lock=self.lock,
            conn_observers=[],
            is_completed=False,
        )

    @dataclass
    class InnerObserver(Observer):
        observer: Observer
        last_ack: Optional[Ack]
        lock: threading.RLock
        conn_observers: List[ConnectableObserver]
        is_completed: bool

        def on_next(self, elem: ElementType) -> Ack:
            ack = self.observer.on_next(elem)
            self.last_ack = ack
            return ack

        def on_error(self, exc: Exception):
            self.observer.on_error(exc)      # todo: check this

        def on_completed(self):
            with self.lock:
                conn_observers = self.conn_observers[1:]
                self.conn_observers = conn_observers
                is_completed = self.is_completed

            if 0 < len(conn_observers):
                if self.last_ack is None:
                    self.conn_observers[0].connect()
                else:
                    class InnerSingle(Single):
                        def on_next(_, elem):
                            self.conn_observers[0].connect()

                        def on_error(_, exc: Exception):
                            pass

                    self.last_ack.subscribe(InnerSingle())

            elif is_completed:
                self.observer.on_completed()

    def on_next(self, elem: ElementType):
        obs_list: List[Observable] = [self.selector(e) for e in elem]

        if len(obs_list) == 0:
            return continue_ack

        # generate a connectable observer for each observer
        def gen_connectable_observer():
            for _ in obs_list:
                conn_observer = ConnectableObserver(
                    underlying=self.inner_observer,
                    scheduler=self.scheduler,
                )
                yield conn_observer

        conn_observers = list(gen_connectable_observer())

        with self.lock:
            prev_conn_observers = self.inner_observer.conn_observers
            self.inner_observer.conn_observers = self.inner_observer.conn_observers + conn_observers

        first_obs = obs_list[0]
        if len(prev_conn_observers) == 0:
            # conn_observers[0] is not used in this case
            first_conn_observer = self.inner_observer

        else:
            first_conn_observer = conn_observers[0]

        other_obs = obs_list[1:]
        other_conn_observers = conn_observers[1:]

        def observe_on_subscribe_scheduler(_, __):
            first_obs.observe(self.observer_info.copy(observer=first_conn_observer))

            for obs, conn_observer in zip(other_obs, other_conn_observers):
                obs.observe(self.observer_info.copy(observer=conn_observer))

        disposable = self.subscribe_scheduler.schedule(observe_on_subscribe_scheduler)
        self.composite_disposable.add(disposable)

        return continue_ack

    def on_error(self, exc):
        self.observer.on_error(exc)

    def on_completed(self):
        with self.lock:
            conn_observers = self.inner_observer.conn_observers
            self.inner_observer.is_completed = True

        if len(conn_observers) == 0:
            self.observer.on_completed()
