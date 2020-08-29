from dataclasses import dataclass
from typing import Callable, Any

from rxbp.acknowledgement.stopack import stop_ack
from rxbp.observer import Observer
from rxbp.typing import ElementType


@dataclass
class TakeWhileObserver(Observer):
    observer: Observer
    predicate: Callable[[Any], bool]

    def __post_init__(self):
        self.take_while_completed = False

    def on_next(self, elem: ElementType):
        def gen_filtered_values():
            for value in elem:
                if not self.predicate(value):
                    self.take_while_completed = True
                    break

                yield value

        try:
            filtered_values = list(gen_filtered_values())
        except Exception as exc:
            self.observer.on_error(exc)
            return stop_ack

        ack = self.observer.on_next(filtered_values)

        if self.take_while_completed:
            self.observer.on_completed()
            return stop_ack

        return ack

    def on_error(self, exc):
        return self.observer.on_error(exc)

    def on_completed(self):
        if not self.take_while_completed:
            return self.observer.on_completed()
