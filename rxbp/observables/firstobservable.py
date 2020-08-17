from dataclasses import dataclass
from traceback import FrameSummary
from typing import List

from rxbp.observable import Observable
from rxbp.observerinfo import ObserverInfo
from rxbp.observers.firstobserver import FirstObserver
from rxbp.utils.tooperatorexception import to_operator_exception


@dataclass
class FirstObservable(Observable):
    source: Observable
    stack: List[FrameSummary]

    def observe(self, observer_info: ObserverInfo):
        try:
            return self.source.observe(observer_info.copy(
                observer=FirstObserver(
                    observer=observer_info.observer,
                    stack=self.stack,
                ),
            ))

        except Exception:
            raise Exception(to_operator_exception(
                message=f'something went wrong when observing {self.source}',
                stack=self.stack,
            ))
