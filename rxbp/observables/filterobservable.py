import functools
from typing import Callable, Any

from rxbp.ack.ack import Ack
from rxbp.ack.ackimpl import stop_ack
from rxbp.ack.merge import _merge
from rxbp.observerinfo import ObserverInfo
from rxbp.observers.filterobserver import FilterObserver
from rxbp.selectors.selectionmsg import select_next, select_completed
from rxbp.observable import Observable
from rxbp.observer import Observer
from rxbp.scheduler import Scheduler
from rxbp.observablesubjects.publishosubject import PublishOSubject
from rxbp.typing import ElementType


class FilterObservable(Observable):
    def __init__(self, source: Observable, predicate: Callable[[Any], bool], scheduler: Scheduler):
        super().__init__()

        self.source = source
        self.predicate = predicate
        self.scheduler = scheduler

    def observe(self, observer_info: ObserverInfo):
        observer = FilterObserver(
            observer=observer_info.observer,
            predicate=self.predicate,
            scheduler=self.scheduler,
        )
        filter_subscription = observer_info.copy(observer)
        return self.source.observe(filter_subscription)
