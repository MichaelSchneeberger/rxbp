from typing import Callable, Any

from rxbp.observable import Observable
from rxbp.observablesubjects.publishosubject import PublishOSubject
from rxbp.observerinfo import ObserverInfo
from rxbp.observers.filterobserver import FilterObserver
from rxbp.scheduler import Scheduler


class FilterObservable(Observable):
    def __init__(self, source: Observable, predicate: Callable[[Any], bool], scheduler: Scheduler):
        super().__init__()

        self.source = source
        self.predicate = predicate
        self.scheduler = scheduler

        self.selector = PublishOSubject(scheduler=scheduler)

    def observe(self, observer_info: ObserverInfo):
        observer = FilterObserver(
            observer=observer_info.observer,
            predicate=self.predicate,
            selector=self.selector,
        )
        filter_subscription = observer_info.copy(observer)
        return self.source.observe(filter_subscription)
