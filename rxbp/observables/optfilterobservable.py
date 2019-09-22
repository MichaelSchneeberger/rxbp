import functools
from typing import Callable, Any

from rxbp.ack.ack import Ack
from rxbp.ack.ackimpl import stop_ack
from rxbp.ack.merge import _merge
from rxbp.observerinfo import ObserverInfo
from rxbp.selectors.selectionmsg import select_next, select_completed
from rxbp.observable import Observable
from rxbp.observer import Observer
from rxbp.scheduler import Scheduler
from rxbp.observablesubjects.publishosubject import PublishOSubject
from rxbp.typing import ElementType


class OptFilterObservable(Observable):
    def __init__(self, source: Observable, predicate: Callable[[Any], bool]):

        super().__init__()

        self.source = source
        self.predicate = predicate

    def observe(self, observer_info: ObserverInfo):
        observer = observer_info.observer
        predicate = self.predicate

        class FilterObserver(Observer):
            def on_next(self, elem: ElementType):
                def gen_filtered_iterable():
                    for e in elem:
                        if predicate(e):
                            yield e

                ack = observer.on_next(gen_filtered_iterable())
                return ack

            def on_error(self, exc):
                return observer.on_error(exc)

            def on_completed(self):
                return observer.on_completed()

        filter_subscription = observer_info.copy(FilterObserver())
        return self.source.observe(filter_subscription)
