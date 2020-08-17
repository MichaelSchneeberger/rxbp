from rxbp.ack.continueack import continue_ack
from rxbp.ack.stopack import stop_ack
from rxbp.observer import Observer
from rxbp.typing import ElementType


class ToListObserver(Observer):
    def __init__(
            self,
            observer: Observer,
    ):
        self.observer = observer

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