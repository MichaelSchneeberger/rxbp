import threading
from typing import Callable, Any

from rxbp.ack.ackbase import AckBase
from rxbp.ack.ackimpl import continue_ack
from rxbp.ack.single import Single
from rxbp.observable import Observable
from rxbp.observer import Observer
from rxbp.observerinfo import ObserverInfo
from rxbp.observers.connectableobserver import ConnectableObserver
from rxbp.scheduler import Scheduler
from rxbp.typing import ElementType


class FlatMapNoBackpressureObservable(Observable):
    def __init__(
            self,
            source: Observable,
            selector: Callable[[Any], Observable],
            scheduler: Scheduler,
            subscribe_scheduler: Scheduler,
    ):
        super().__init__()

        self.source = source
        self.selector = selector
        self.scheduler = scheduler
        self.subscribe_scheduler = subscribe_scheduler

    def observe(self, observer_info: ObserverInfo):
        observer = observer_info.observer
        scheduler = self.scheduler
        subscribe_scheduler = self.subscribe_scheduler

        class ConcatObserver(Observer):
            def __init__(
                    self,
                    observer: Observer,
                    selector: Callable[[Any], Observable],
            ):
                self.observer = observer
                self.selector = selector

                self.lock = threading.RLock()
                self.conn_observers = []
                self.is_completed = False
                self.last_ack = None

                class InnerObserver(Observer):
                    def __init__(self):
                        self.last_ack = None

                    def on_next(_, elem: ElementType) -> AckBase:
                        ack = observer.on_next(elem)
                        self.last_ack = ack
                        return ack

                    def on_error(_, exc: Exception):
                        pass

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
                elem_list = [self.selector(e) for e in elem]

                def gen_conn_observer():
                    for _ in elem_list:
                        conn_observer = ConnectableObserver(
                            underlying=self.inner_observer,
                            scheduler=scheduler,
                            subscribe_scheduler=subscribe_scheduler,
                            is_volatile=observer_info.is_volatile,
                        )
                        # v.observe(observer_info.copy(conn_observer))
                        yield conn_observer

                conn_observers = list(gen_conn_observer())

                with self.lock:
                    prev_conn_observers = self.conn_observers
                    self.conn_observers = self.conn_observers + conn_observers

                if len(prev_conn_observers) == 0:
                    elem_list[0].observe(observer_info.copy(self.inner_observer))
                else:
                    elem_list[0].observe(observer_info.copy(conn_observers[0]))

                for obs, conn_observer in zip(elem_list[1:], conn_observers[1:]):
                    obs.observe(observer_info.copy(conn_observer))

                return continue_ack

            def on_error(self, exc):
                observer.on_error(exc)

            def on_completed(self):
                with self.lock:
                    conn_observers = self.conn_observers
                    self.is_connected = True

                if len(conn_observers) == 0:
                    observer.on_completed()

        concat_observer = ConcatObserver(observer=observer, selector=self.selector)
        disposable = self.source.observe(observer_info.copy(observer=concat_observer))
        # print('subscribe')
        return disposable
