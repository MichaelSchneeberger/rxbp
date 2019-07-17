from abc import ABC, abstractmethod


class Single(ABC):
    @abstractmethod
    def on_next(self, elem):
        ...

    @abstractmethod
    def on_error(self, exc: Exception):
        ...
