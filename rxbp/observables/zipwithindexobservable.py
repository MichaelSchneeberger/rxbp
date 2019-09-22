from typing import Callable, Any

from rxbp.observable import Observable
from rxbp.observer import Observer
from rxbp.observerinfo import ObserverInfo
from rxbp.typing import ElementType


class ZipWithIndexObservable(Observable):
    def __init__(self, source, selector: Callable[[Any, int], Any]):
        self.source = source
        self.selector = (lambda v, i: (v, i)) if selector is None else selector

    def observe(self, observer_info: ObserverInfo):
        observer = observer_info.observer
        selector = self.selector

        class ZipCountObserver(Observer):
            def __init__(self):
                self.count = 0

            def on_next(self, elem: ElementType):
                def map_gen():
                    for v in elem:
                        result = selector(v, self.count)
                        self.count += 1
                        yield result

                return observer.on_next(map_gen())

            def on_error(self, exc):
                return observer.on_error(exc)

            def on_completed(self):
                return observer.on_completed()

        map_observer = ObserverInfo(ZipCountObserver(), is_volatile=observer_info.is_volatile)
        return self.source.observe(map_observer)
