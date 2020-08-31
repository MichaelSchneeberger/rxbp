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

    def __post_init__(self):
        self._subjects = [PublishObservableSubject() for _ in self.sources]

    def observe(self, observer_info: ObserverInfo):

        """
        sources[0] ------------------------> Subject --
                                                       \
        sources[1] -> ConnectableObserver -> Subject -----> ConcatObserver
        ...                                            /
        sources[n] -> ConnectableObserver -> Subject --
        """

        conn_observers = [ConnectableObserver(
            underlying=subject,
            scheduler=self.scheduler,
            # subscribe_scheduler=self._subscribe_scheduler,
            # is_volatile=observer_info.is_volatile,
        ) for subject in self._subjects[1:]]

        iter_conn_obs = iter(conn_observers)

        concat_observer = ConcatObserver(
            source=observer_info.observer,
            connectables=iter_conn_obs,
        )
        concat_observer_info = init_observer_info(concat_observer)

        for subject in self._subjects:
            subject.observe(concat_observer_info)

        def gen_disposable_from_observer():
            for source, conn_observer in zip(self.sources[1:], conn_observers):
                yield source.observe(init_observer_info(
                    observer=conn_observer,
                    is_volatile=observer_info.is_volatile,
                ))

        others_disposables = gen_disposable_from_observer()

        first_disposable = self.sources[0].observe(init_observer_info(self._subjects[0]))

        return CompositeDisposable(first_disposable, *others_disposables)
