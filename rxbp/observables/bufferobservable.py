from typing import Optional

from rxbp.observable import Observable
from rxbp.observerinfo import ObserverInfo
from rxbp.observers.backpressurebufferedobserver import BackpressureBufferedObserver
from rxbp.scheduler import Scheduler


class BufferObservable(Observable):
    def __init__(
            self,
            source: Observable,
            scheduler: Scheduler,
            subscribe_scheduler: Scheduler,
            buffer_size: Optional[int],
    ):
        self.source = source
        self.buffer_size = buffer_size
        self.scheduler = scheduler
        self.subscribe_scheduler = subscribe_scheduler

    def observe(self, observer_info: ObserverInfo):
        observer = observer_info.observer

        buffered_observer = BackpressureBufferedObserver(
            underlying=observer, scheduler=self.scheduler,
            subscribe_scheduler=self.subscribe_scheduler,
            buffer_size=self.buffer_size,
        )
        disposable = self.source.observe(ObserverInfo(observer=buffered_observer))
        return disposable
