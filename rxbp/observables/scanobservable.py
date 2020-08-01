from typing import Callable, Any

from rxbp.observable import Observable
from rxbp.observerinfo import ObserverInfo
from rxbp.observers.scanobserver import ScanObserver


class ScanObservable(Observable):
    def __init__(
            self,
            source: Observable,
            func: Callable[[Any, Any], Any],
            initial: Any,
    ):
        self.source = source
        self.func = func
        self.initial = initial

    def observe(self, observer_info: ObserverInfo):
        return self.source.observe(observer_info.copy(
            observer=ScanObserver(
                observer=observer_info.observer,
                func=self.func,
                initial=self.initial,
            ),
        ))
