from typing import List

from rx.disposable import CompositeDisposable

from rxbp.ack.acksubject import AckSubject
from rxbp.ack.operators.merge import _merge
from rxbp.ack.single import Single
from rxbp.ack.stopack import stop_ack
from rxbp.observable import Observable
from rxbp.observables.mapobservable import MapObservable
from rxbp.observables.maptoiteratorobservable import MapToIteratorObservable
from rxbp.observablesubjects.publishosubject import PublishOSubject
from rxbp.observer import Observer
from rxbp.observerinfo import ObserverInfo
from rxbp.observers.connectableobserver import ConnectableObserver
from rxbp.scheduler import Scheduler
from rxbp.selectors.selectionmsg import select_next, select_completed
from rxbp.typing import ElementType


class ConcatObservable(Observable):
    def __init__(self, sources: List[Observable], scheduler: Scheduler, subscribe_scheduler: Scheduler):
        super().__init__()

        self._sources = sources
        self._scheduler = scheduler
        self._subscribe_scheduler = subscribe_scheduler

        self._subjects = [PublishOSubject(scheduler=scheduler) for _ in sources]

    @property
    def selectors(self):
        return [MapToIteratorObservable(subject, lambda v: [select_next, select_completed]) for subject in self._subjects]

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
                    class _(Single):
                        def on_next(self, elem):
                            next_source = next(iter_conn_obs)
                            next_source.connect()

                        def on_error(self, exc: Exception):
                            pass

                    if self.ack is None or self.ack.is_sync:
                        next_source = next(iter_conn_obs)
                        next_source.connect()
                    else:
                        self.ack.subscribe(_())
                        
                except StopIteration:
                    observer.on_completed()

        """
        sources[0] ------------------------> Subject --
                                                       \
        sources[1] -> ConnectableObserver -> Subject -----> ConcatObserver
        ...                                            /
        sources[n] -> ConnectableObserver -> Subject --
        """

        concat_observer = ConcatObserver()
        concat_observer_info = ObserverInfo(concat_observer)

        for subject in self._subjects:
            subject.observe(concat_observer_info)

        conn_observers = [ConnectableObserver(
            underlying=subject,
            scheduler=self._scheduler,
            subscribe_scheduler=self._subscribe_scheduler,
            is_volatile=observer_info.is_volatile,
        ) for subject in self._subjects[1:]]

        iter_conn_obs = iter(conn_observers)

        def gen_disposable_from_observer():
            for source, conn_observer in zip(self._sources[1:], conn_observers):
                yield source.observe(ObserverInfo(
                    observer=conn_observer,
                    is_volatile=observer_info.is_volatile,
                ))

        others_disposables = gen_disposable_from_observer()

        first_disposable = self._sources[0].observe(ObserverInfo(self._subjects[0]))

        return CompositeDisposable(first_disposable, *others_disposables)
