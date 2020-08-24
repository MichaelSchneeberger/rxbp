from dataclasses import dataclass
from typing import List, Optional

from rxbp.ack.continueack import continue_ack
from rxbp.ack.mixins.ackmixin import AckMixin
from rxbp.mixins.flowablesubscribemixin import FlowableSubscribeMixin
from rxbp.observer import Observer
from rxbp.scheduler import Scheduler
from rxbp.schedulers.trampolinescheduler import TrampolineScheduler
from rxbp.typing import ElementType


def to_iterator(source: FlowableSubscribeMixin, scheduler: Scheduler = None):
    @dataclass
    class ToIteratorObserver(Observer):
        received: List[ElementType]
        is_completed: bool
        exception: Optional[Exception]

        def on_next(self, elem: ElementType) -> AckMixin:
            if not isinstance(elem, list):
                elem = list(elem)

            self.received.append(elem)
            return continue_ack

        def on_error(self, exc: Exception):
            self.exception = exc

        def on_completed(self):
            self.is_completed = True

    observer = ToIteratorObserver(
        received=[],
        is_completed=False,
        exception=None,
    )
    subscribe_scheduler = TrampolineScheduler()
    scheduler = scheduler or subscribe_scheduler

    source.subscribe(
        observer=observer,
        scheduler=scheduler,
        subscribe_scheduler=subscribe_scheduler,
    )

    def gen():
        while True:
            while True:
                if len(observer.received):
                    break

                if observer.is_completed:
                    return  # StopIteration

                if observer.exception is not None:
                    raise observer.exception

                scheduler.sleep(0.1)

            yield from observer.received.pop(0)

    return gen()
