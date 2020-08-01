from dataclasses import dataclass

import rx
from rx.core.typing import Disposable, Scheduler

from rxbp.observable import Observable
from rxbp.observerinfo import ObserverInfo
from rxbp.observers.evictingbufferedobserver import EvictingBufferedObserver
from rxbp.overflowstrategy import OverflowStrategy


@dataclass
class FromRxEvictingObservable(Observable):
    batched_source: rx.typing.Observable
    scheduler: Scheduler
    subscribe_scheduler: Scheduler
    overflow_strategy: OverflowStrategy

    def observe(self, observer_info: ObserverInfo) -> Disposable:
        observer = EvictingBufferedObserver(
            observer=observer_info.observer,
            scheduler=self.scheduler,
            subscribe_scheduler=self.subscribe_scheduler,
            strategy=self.overflow_strategy,
        )

        return self.batched_source.subscribe(
            on_next=observer.on_next,
            on_error=observer.on_error,
            on_completed=observer.on_completed,
            scheduler=self.scheduler,
        )
