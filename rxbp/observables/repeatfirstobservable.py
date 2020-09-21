from dataclasses import dataclass

from rxbp.observable import Observable
from rxbp.observerinfo import ObserverInfo
from rxbp.observers.repeatfirstobserver import RepeatFirstObserver
from rxbp.scheduler import Scheduler


@dataclass
class RepeatFirstObservable(Observable):
    source: Observable
    scheduler: Scheduler

    def observe(self, observer_info: ObserverInfo):
        return self.source.observe(
            observer_info=observer_info.copy(observer=RepeatFirstObserver(
                next_observer=observer_info.observer,
                scheduler=self.scheduler,
            ))
        )
