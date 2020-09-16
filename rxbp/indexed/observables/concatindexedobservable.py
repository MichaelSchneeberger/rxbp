from dataclasses import dataclass
from typing import List

from rx.disposable import CompositeDisposable

from rxbp.init.initobserverinfo import init_observer_info
from rxbp.observable import Observable
from rxbp.observables.maptoiteratorobservable import MapToIteratorObservable
from rxbp.observablesubjects.publishobservablesubject import PublishObservableSubject
from rxbp.observer import Observer
from rxbp.observerinfo import ObserverInfo
from rxbp.observers.concatobserver import ConcatObserver
from rxbp.observers.connectableobserver import ConnectableObserver
from rxbp.scheduler import Scheduler
from rxbp.indexed.selectors.selectnext import select_next
from rxbp.indexed.selectors.selectcompleted import select_completed
from rxbp.typing import ElementType


@dataclass
class ConcatIndexedObservable(Observable):
    sources: List[Observable]
    scheduler: Scheduler
    subscribe_scheduler: Scheduler

    def __post_init__(self):
        self.subjects = [PublishObservableSubject() for _ in self.sources]

    @property
    def selectors(self):
        return [MapToIteratorObservable(subject, lambda v: [select_next, select_completed]) for subject in self.subjects]

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
        ) for subject in self.subjects[1:]]

        connectables = iter(conn_observers)

        concat_observer = ConcatObserver(
            next_observer=observer_info.observer,
            connectables=connectables
        )

        concat_observer_info = observer_info.copy(
            observer=concat_observer,
        )

        for subject in self.subjects:
            subject.observe(concat_observer_info)

        def gen_disposable_from_observer():
            for source, conn_observer in zip(self.sources[1:], conn_observers):
                yield source.observe(observer_info.copy(
                    observer=conn_observer,
                ))

        others_disposables = gen_disposable_from_observer()

        first_disposable = self.sources[0].observe(observer_info.copy(
            observer=self.subjects[0],
        ))

        return CompositeDisposable(first_disposable, *others_disposables)
