from dataclasses import dataclass
from traceback import FrameSummary
from typing import List

from rxbp.observable import Observable
from rxbp.observerinfo import ObserverInfo
from rxbp.observers.firstobserver import FirstObserver


@dataclass
class FirstObservable(Observable):
    source: Observable
    stack: List[FrameSummary]

    def observe(self, observer_info: ObserverInfo):
        return self.source.observe(observer_info.copy(
            observer=FirstObserver(
                observer=observer_info.observer,
                stack=self.stack,
            ),
        ))
