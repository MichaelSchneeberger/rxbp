from typing import Callable, Any

from rxbp.observable import Observable
from rxbp.observerinfo import ObserverInfo
from rxbp.observers.firstordefaultobserver import FirstOrDefaultObserver


class FirstOrDefaultObservable(Observable):
    def __init__(
            self,
            source: Observable,
            lazy_val: Callable[[], Any],
    ):
        self.source = source
        self.lazy_val = lazy_val

    def observe(self, observer_info: ObserverInfo):
        return self.source.observe(observer_info.copy(
            observer=FirstOrDefaultObserver(
                observer=observer_info.observer,
                lazy_val=self.lazy_val,
            )
        ))
