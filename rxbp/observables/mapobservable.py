from typing import Callable, Any

from rxbp.observable import Observable
from rxbp.observer import Observer
from rxbp.scheduler import Scheduler


class MapObservable(Observable):
    def __init__(self, source: Observable, selector: Callable[[Any], Any]):
        super().__init__()

        self.source = source
        self.selector = selector

    def observe(self, observer: Observer):
        def on_next(v):

            # `map` does not consume elements from buffer, it is not its responsibility to try catch an exception
            def map_gen():
                for e in v():
                    yield self.selector(e)

            return observer.on_next(map_gen)

        class MapObserver(Observer):
            def on_next(self, v):
                return on_next(v)

            def on_error(self, exc):
                return observer.on_error(exc)

            def on_completed(self):
                return observer.on_completed()

        map_observer = MapObserver()
        return self.source.observe(map_observer)
