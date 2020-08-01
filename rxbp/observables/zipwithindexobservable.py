from typing import Callable, Any

from rxbp.init.initobserverinfo import init_observer_info
from rxbp.observable import Observable
from rxbp.observerinfo import ObserverInfo
from rxbp.observers.zipwithindexobserver import ZipCountObserver


class ZipWithIndexObservable(Observable):
    def __init__(
            self,
            source: Observable,
            selector: Callable[[Any, int], Any],
    ):
        self.source = source
        self.selector = (lambda v, i: (v, i)) if selector is None else selector

    def observe(self, observer_info: ObserverInfo):
        return self.source.observe(init_observer_info(
            observer=ZipCountObserver(
                observer=observer_info.observer,
                selector=self.selector,
            ),
            is_volatile=observer_info.is_volatile,
        ))
