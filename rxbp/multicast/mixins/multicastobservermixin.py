from abc import ABC, abstractmethod

from rxbp.multicast.typing import MultiCastItem


class MultiCastObserverMixin(ABC):
    @abstractmethod
    def on_next(self, item: MultiCastItem) -> None:
        ...

    @abstractmethod
    def on_error(self, exc: Exception) -> None:
        ...

    @abstractmethod
    def on_completed(self) -> None:
        ...
