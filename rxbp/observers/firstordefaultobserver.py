from typing import Callable, Any

from rxbp.acknowledgement.continueack import continue_ack
from rxbp.acknowledgement.stopack import stop_ack
from rxbp.observer import Observer
from rxbp.typing import ElementType


class FirstOrDefaultObserver(Observer):
    def __init__(
            self,
            observer: Observer,
            lazy_val: Callable[[], Any],
    ):
        super().__init__()

        self.observer = observer
        self.lazy_val = lazy_val

        self.is_first = True

    def on_next(self, elem: ElementType):
        try:
            first_elem = next(iter(elem))
        except StopIteration:
            return continue_ack

        self.is_first = False
        self.observer.on_next([first_elem])
        self.observer.on_completed()
        return stop_ack

    def on_error(self, exc):
        return self.observer.on_error(exc)

    def on_completed(self):
        if self.is_first:
            try:
                self.observer.on_next(self.lazy_val())
                self.observer.on_completed()
            except Exception as exc:
                self.observer.on_error(exc)
