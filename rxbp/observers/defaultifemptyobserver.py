import types
from dataclasses import dataclass
from typing import Callable, Any

from rxbp.acknowledgement.continueack import continue_ack
from rxbp.observer import Observer
from rxbp.typing import ElementType


@dataclass
class DefaultIfEmptyObserver(Observer):
    next_observer: Observer
    lazy_val: Callable[[], Any]

    def __post_init__(self):
        self.is_first = True

    def on_next(self, elem: ElementType):
        if isinstance(elem, list):
            materialized = elem
        else:
            materialized = list(elem)

        if len(materialized) == 0:
            return continue_ack

        self.is_first = False

        self.on_next = types.MethodType(lambda _, v: self.next_observer.on_next(v), self)  # type: ignore

        return self.next_observer.on_next(materialized)

    def on_error(self, exc):
        return self.next_observer.on_error(exc)

    def on_completed(self):
        if self.is_first:
            try:
                self.next_observer.on_next([self.lazy_val()])
                self.next_observer.on_completed()
            except Exception as exc:
                self.next_observer.on_error(exc)
        else:
            self.next_observer.on_completed()
