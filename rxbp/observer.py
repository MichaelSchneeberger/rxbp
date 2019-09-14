from abc import ABC, abstractmethod

from rxbp.ack.ackbase import AckBase
from rxbp.typing import ValueType


class Observer(ABC):
    @abstractmethod
    def on_next(self, elem: ValueType) -> AckBase:
        ...

    @abstractmethod
    def on_error(self, exc: Exception):
        ...

    @abstractmethod
    def on_completed(self):
        ...
