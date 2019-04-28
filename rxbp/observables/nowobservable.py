from typing import Any

from rxbp.observable import Observable
from rxbp.observer import Observer
from rxbp.scheduler import Scheduler


# todo delete?
class NowObservable(Observable):
    def __init__(self, elem: Any, subscribe_scheduler: Scheduler):
        self.subscribe_scheduler = subscribe_scheduler
        self.elem = elem

    def observe(self, observer: Observer):
        def action(_, __):
            def gen_single_elem():
                yield self.elem

            observer.on_next(gen_single_elem)
            observer.on_completed()

        return self.subscribe_scheduler.schedule(action)

