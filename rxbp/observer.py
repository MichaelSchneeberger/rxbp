from abc import ABC, abstractmethod

from rxbp.ack.mixins.ackmixin import AckMixin
from rxbp.typing import ElementType


class Observer(ABC):
    """
    An Observer interface is defined by three methods `on_next`, `on_error` and `on_completed`. It is responsible to
    forward information from data source to data sink (e.g. downstream). It implements back-pressure (or flow control)
    by having `on_next` return an acknowledgment.

    If using an rxbp Observable, then the following conventions must be respected:

    """

    @abstractmethod
    def on_next(self, elem: ElementType) -> AckMixin:
        """
        This function is called to send some information downstream. The function must return an acknowledgment,
        which either continues streaming object, stops streaming objects or back-pressure source from sending
        the next element.
        """

        ...

    @abstractmethod
    def on_error(self, exc: Exception):
        ...

    @abstractmethod
    def on_completed(self):
        ...
