from abc import ABC, abstractmethod

from rxbp.multicast.typing import MultiCastItem


class MultiCastObserverMixin(ABC):
    """
    The following conventions should hold:

    - `on_next` method can be called multiple times simultaneously;
      the order does not need to be guaranteed
    - `on_next` method should call the next `on_next` without delay
      (e.g. without scheduler)
    - `on_completed` method is called only after the last `on_next`
      method call returned
    - `on_error` method can be called any time
    - either `on_completed` or `on_error` or none of them should be
      called
    """

    @abstractmethod
    def on_next(self, elem: MultiCastItem) -> None:
        ...

    @abstractmethod
    def on_error(self, exc: Exception) -> None:
        ...

    @abstractmethod
    def on_completed(self) -> None:
        ...
