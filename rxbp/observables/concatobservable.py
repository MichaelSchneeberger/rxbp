from dataclasses import dataclass
from typing import List

from rx.disposable import CompositeDisposable

from rxbp.init.initobserverinfo import init_observer_info
from rxbp.observable import Observable
from rxbp.observablesubjects.publishobservablesubject import PublishObservableSubject
from rxbp.observerinfo import ObserverInfo
from rxbp.observers.concatobserver import ConcatObserver
from rxbp.observers.connectableobserver import ConnectableObserver
from rxbp.scheduler import Scheduler


@dataclass
class ConcatObservable(Observable):
    sources: List[Observable]
    scheduler: Scheduler
    subscribe_scheduler: Scheduler

    def observe(self, observer_info: ObserverInfo):

        """
        sources[0] ------------------------> Subject --
                                                       \
        sources[1] -> ConnectableObserver -> Subject -----> ConcatObserver
        ...                                            /
        sources[n] -> ConnectableObserver -> Subject --
        """

        conn_observers = [ConnectableObserver(
            underlying=None,
        ) for _ in self.sources]

        iter_conn_obs = iter(conn_observers)

        concat_observer = ConcatObserver(
            next_observer=observer_info.observer,
            connectables=iter_conn_obs,
        )

        for conn_observer in conn_observers:
            conn_observer.underlying = concat_observer

        def gen_disposable_from_observer():
            for source, conn_observer in zip(self.sources[1:], conn_observers):
                yield source.observe(observer_info.copy(
                    observer=conn_observer,
                ))

        others_disposables = gen_disposable_from_observer()

        first_disposable = self.sources[0].observe(observer_info.copy(
            observer=concat_observer,
        ))

        return CompositeDisposable(first_disposable, *others_disposables)
