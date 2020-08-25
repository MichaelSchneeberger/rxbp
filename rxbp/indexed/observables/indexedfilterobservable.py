from dataclasses import dataclass
from typing import Callable, Any

from rxbp.indexed.observers.indexedfilterobserver import IndexedFilterObserver
from rxbp.observable import Observable
from rxbp.observablesubjects.publishosubject import PublishOSubject
from rxbp.observerinfo import ObserverInfo


@dataclass
class IndexedFilterObservable(Observable):
    source: Observable
    predicate: Callable[[Any], bool]
    selector_subject: PublishOSubject

    def observe(self, observer_info: ObserverInfo):
        subscription = observer_info.copy(
            observer=IndexedFilterObserver(
                observer=observer_info.observer,
                predicate=self.predicate,
                selector_subject=self.selector_subject,
            ),
        )
        return self.source.observe(subscription)
