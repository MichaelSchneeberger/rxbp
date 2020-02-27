import threading
from typing import Callable, Any, List

from rxbp.ack.continueack import continue_ack
from rxbp.ack.mixins.ackmixin import AckMixin
from rxbp.ack.single import Single
from rxbp.observable import Observable
from rxbp.observer import Observer
from rxbp.observerinfo import ObserverInfo
from rxbp.observers.connectableobserver import ConnectableObserver
from rxbp.scheduler import Scheduler
from rxbp.typing import ElementType


class FlatConcatNoBackpressureObserver(Observer):
    def __init__(
            self,
            observer: Observer,
            selector: Callable[[Any], Observable],
            scheduler: Scheduler,
            subscribe_scheduler: Scheduler,
            is_volatile: bool,
    ):
        self.observer = observer
        self.selector = selector
        self.scheduler = scheduler
        self.subscribe_scheduler = subscribe_scheduler
        self.is_volatile = is_volatile

        self.lock = threading.RLock()
        self.conn_observers = []
        self.is_completed = False
        self.last_ack = None

        class InnerObserver(Observer):
            def __init__(self):
                self.last_ack = None

            def on_next(_, elem: ElementType) -> AckMixin:
                ack = observer.on_next(elem)
                self.last_ack = ack
                return ack

            def on_error(_, exc: Exception):
                observer.on_error(exc)      # todo: check this

            def on_completed(_):
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
                    observer.on_completed()

        self.inner_observer = InnerObserver()

    def on_next(self, elem: ElementType):
        obs_list: List[Observable] = [self.selector(e) for e in elem]

        # generate a connectable observer for each observer
        def gen_connectable_observer():
            for _ in obs_list:
                conn_observer = ConnectableObserver(
                    underlying=self.inner_observer,
                    scheduler=self.scheduler,
                    subscribe_scheduler=self.subscribe_scheduler,
                    is_volatile=self.is_volatile,
                )
                yield conn_observer

        conn_observers = list(gen_connectable_observer())

        with self.lock:
            prev_conn_observers = self.conn_observers
            self.conn_observers = self.conn_observers + conn_observers

        # observe the received observables immediately
        # - observe the first observable to the inner observer
        # - observe the other observables to each of the connectable observers
        #   (the connectable observers backpressure the source until its connected)

        if len(prev_conn_observers) == 0:
            obs_list[0].observe(ObserverInfo(observer=self.inner_observer, is_volatile=self.is_volatile))
        else:
            obs_list[0].observe(ObserverInfo(observer=conn_observers[0], is_volatile=self.is_volatile))

        for obs, conn_observer in zip(obs_list[1:], conn_observers[1:]):
            obs.observe(ObserverInfo(observer=conn_observer, is_volatile=self.is_volatile))

        return continue_ack

    def on_error(self, exc):
        self.observer.on_error(exc)

    def on_completed(self):
        with self.lock:
            conn_observers = self.conn_observers
            # self.is_connected = True
            self.is_completed = True

        if len(conn_observers) == 0:
            self.observer.on_completed()
