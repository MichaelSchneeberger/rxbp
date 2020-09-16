from dataclasses import dataclass
from typing import Callable, Any

from rxbp.indexed.observers.indexedfilterobserver import IndexedFilterObserver
from rxbp.observable import Observable
from rxbp.observablesubjects.publishobservablesubject import PublishObservableSubject
from rxbp.observerinfo import ObserverInfo


@dataclass
class FilterIndexedObservable(Observable):
    source: Observable
    predicate: Callable[[Any], bool]
    selector_subject: PublishObservableSubject

    def observe(self, observer_info: ObserverInfo):
        subscription = observer_info.copy(
            observer=IndexedFilterObserver(
                observer=observer_info.observer,
                predicate=self.predicate,
                selector_subject=self.selector_subject,
            ),
        )
        return self.source.observe(subscription)
