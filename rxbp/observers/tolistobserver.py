from dataclasses import dataclass

from rxbp.acknowledgement.continueack import continue_ack
from rxbp.acknowledgement.stopack import stop_ack
from rxbp.observer import Observer
from rxbp.typing import ElementType


@dataclass
class ToListObserver(Observer):
    observer: Observer

    def __post_init__(self):
        self.queue = []

    def on_next(self, elem: ElementType):
        if isinstance(elem, list):
            elem_list = elem

        else:
            try:
                elem_list = list(elem)
            except Exception as exc:
                self.on_error(exc)
                return stop_ack

        self.queue += elem_list

        return continue_ack

    def on_error(self, exc):
        return self.observer.on_error(exc)

    def on_completed(self):
        _ = self.observer.on_next([self.queue])
        self.observer.on_completed()