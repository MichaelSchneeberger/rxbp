from abc import ABC, abstractmethod


class Observer(ABC):
    @abstractmethod
    def on_next(self, v):
        ...

    @abstractmethod
    def on_error(self, err: Exception):
        ...

    @abstractmethod
    def on_completed(self):
        ...
