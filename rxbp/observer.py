from abc import ABC, abstractmethod

from rxbp.typing import ElementType


class Observer(ABC):
    @abstractmethod
    def on_next(self, elem: ElementType):
        ...

    @abstractmethod
    def on_error(self, exc: Exception):
        ...

    @abstractmethod
    def on_completed(self):
        ...
