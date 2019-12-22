from typing import Callable, Any

from rxbp.observable import Observable
from rxbp.observer import Observer
from rxbp.observerinfo import ObserverInfo
from rxbp.typing import ElementType


class FastFilterObservable(Observable):
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
