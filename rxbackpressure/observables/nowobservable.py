from typing import Any

from rx.concurrency.schedulerbase import SchedulerBase
from rx.core import Disposable

from rxbackpressure.observable import Observable
from rxbackpressure.observer import Observer
from rxbackpressure.scheduler import SchedulerBase


class NowObservable(Observable):
    def __init__(self, source: Observable, elem: Any):
        self.child = source
        self.elem = elem

    def unsafe_subscribe(self, observer: Observer, scheduler: SchedulerBase, subscribe_scheduler: SchedulerBase):
        def action(_, __):
            observer.on_next(self.elem)
            observer.on_completed()
            Disposable.empty()
        subscribe_scheduler.schedule(action)

