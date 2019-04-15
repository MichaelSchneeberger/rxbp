from abc import ABC, abstractmethod


class Observer(ABC):
    @abstractmethod
    def on_next(self, v):
        ...

    @abstractmethod
    def on_error(self, err):
        ...

    @abstractmethod
    def on_completed(self):
        ...
