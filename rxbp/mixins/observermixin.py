from abc import ABC, abstractmethod

from rxbp.acknowledgement.ack import Ack
from rxbp.typing import ElementType


class ObserverMixin(ABC):
    """
    An Observer interface is defined by three methods `on_next`, `on_error` and `on_completed`. It is responsible to
    forward information from data source to data sink (e.g. downstream). It implements back-pressure (or flow control)
    by having `on_next` return an acknowledgment.

    If using an rxbp Observable, then the following conventions must be respected:

    """

    @abstractmethod
    def on_next(self, elem: ElementType) -> Ack:
        """
        This function is called to send information downstream. The function must return an acknowledgment,
        which is either:
        - continue to emit elements
        - stop to send elements (forever)
        - back-pressure the source until continue or stop is received asynchronously
        """

        ...

    @abstractmethod
    def on_error(self, exc: Exception):
        ...

    @abstractmethod
    def on_completed(self):
        ...
