from typing import List

from rx.disposable import CompositeDisposable
from rxbp.observable import Observable
from rxbp.observer import Observer
from rxbp.observerinfo import ObserverInfo
from rxbp.observers.connectableobserver import ConnectableObserver
from rxbp.scheduler import Scheduler
from rxbp.typing import ElementType


class ConcatObservable(Observable):
    def __init__(self, sources: List[Observable], scheduler: Scheduler, subscribe_scheduler: Scheduler):
        super().__init__()

        self._sources = sources
        self._scheduler = scheduler
        self._subscribe_scheduler = subscribe_scheduler

    def observe(self, observer_info: ObserverInfo):
        observer = observer_info.observer

        class ConcatObserver(Observer):
            def __init__(self):
                self.ack = None

            def on_next(self, elem: ElementType):
                self.ack = observer.on_next(elem)
                return self.ack

            def on_error(self, exc):
                observer.on_error(exc)

                for conn_obs in iter_conn_obs:
                    conn_obs.dispose()

            def on_completed(self):
                try:
                    next_source = next(iter_conn_obs)
                    next_source.connect()
                except StopIteration:
                    observer.on_completed()

        concat_observer = ConcatObserver()
        concat_subscription = ObserverInfo(concat_observer, is_volatile=observer_info.is_volatile)

        conn_observers = [ConnectableObserver(
            underlying=concat_observer,
            scheduler=self._scheduler,
            subscribe_scheduler=self._subscribe_scheduler,
            is_volatile=concat_subscription.is_volatile,
        ) for _ in range(len(self._sources) - 1)]

        iter_conn_obs = iter(conn_observers)

        def gen_disposable_from_observer():
            for source, conn_observer in zip(self._sources[1:], conn_observers):
                oi = observer_info.copy(observer=conn_observer)
                yield source.observe(oi)

        disposables = gen_disposable_from_observer()

        first_source = self._sources[0]
        disposable = first_source.observe(concat_subscription)

        return CompositeDisposable(disposable, *disposables)
