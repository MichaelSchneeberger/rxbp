from dataclasses import dataclass
from typing import Callable, Any

from rxbp.acknowledgement.continueack import continue_ack
from rxbp.acknowledgement.stopack import stop_ack
from rxbp.observer import Observer
from rxbp.typing import ElementType


@dataclass
class FirstOrDefaultObserver(Observer):
    observer: Observer
    lazy_val: Callable[[], Any]

    def __post_init__(self):
        self.is_first = True

    def on_next(self, elem: ElementType):
        try:
            first_elem = next(iter(elem))
        except StopIteration:
            return continue_ack
        except Exception as exc:
            self.on_error(exc)
            return

        self.is_first = False
        self.observer.on_next([first_elem])
        self.observer.on_completed()

        return stop_ack

    def on_error(self, exc):
        return self.observer.on_error(exc)

    def on_completed(self):
        if self.is_first:
            try:
                self.observer.on_next([self.lazy_val()])
                self.observer.on_completed()
            except Exception as exc:
                self.observer.on_error(exc)
