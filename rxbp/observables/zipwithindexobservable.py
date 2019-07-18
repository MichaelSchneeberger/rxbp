from typing import Callable, Any

from rxbp.observable import Observable
from rxbp.observer import Observer
from rxbp.observesubscription import ObserveSubscription


class ZipWithIndexObservable(Observable):
    def __init__(self, source, selector: Callable[[Any, int], Any]):
        self.source = source
        self.selector = (lambda v, i: (v, i)) if selector is None else selector

    def observe(self, subscription: ObserveSubscription):
        observer = subscription.observer
        count = [0]

        def on_next(v):
            def map_gen():
                for e in v():
                    result = self.selector(e, count[0])
                    count[0] += 1
                    yield result

            return observer.on_next(map_gen)

        class ZipCountObserver(Observer):
            def on_next(self, v):
                return on_next(v)

            def on_error(self, exc):
                return observer.on_error(exc)

            def on_completed(self):
                return observer.on_completed()

        map_observer = ObserveSubscription(ZipCountObserver(), is_volatile=subscription.is_volatile)
        return self.source.observe(map_observer)
