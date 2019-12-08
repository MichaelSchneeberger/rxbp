import threading
from typing import Callable, Any

from rxbp.ack.ackbase import AckBase
from rxbp.ack.ackimpl import continue_ack
from rxbp.ack.single import Single
from rxbp.multicast.observer.flatmapnobackpressureobserver import FlatMapNoBackpressureObserver
from rxbp.multicast.observer.flatmergenobackpressureobserver import FlatMergeNoBackpressureObserver
from rxbp.observable import Observable
from rxbp.observer import Observer
from rxbp.observerinfo import ObserverInfo
from rxbp.observers.connectableobserver import ConnectableObserver
from rxbp.scheduler import Scheduler
from rxbp.typing import ElementType


class FlatMergeNoBackpressureObservable(Observable):
    def __init__(
            self,
            source: Observable,
            selector: Callable[[Any], Observable],
            scheduler: Scheduler,
            subscribe_scheduler: Scheduler,
    ):
        super().__init__()

        self.source = source
        self.selector = selector
        self.scheduler = scheduler
        self.subscribe_scheduler = subscribe_scheduler

    def observe(self, observer_info: ObserverInfo):
        observer = observer_info.observer
        scheduler = self.scheduler
        subscribe_scheduler = self.subscribe_scheduler

        concat_observer = FlatMergeNoBackpressureObserver(
            observer=observer,
            selector=self.selector,
            scheduler=scheduler,
            subscribe_scheduler=subscribe_scheduler,
            is_volatile=observer_info.is_volatile,
        )
        disposable = self.source.observe(observer_info.copy(observer=concat_observer))
        return disposable
