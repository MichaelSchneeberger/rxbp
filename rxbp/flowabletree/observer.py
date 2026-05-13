from abc import ABC, abstractmethod
from typing import TypeVar

from contcert.typing import ContCert
from rxbp.flowabletree.notifications import (
    Notification,
)


E = TypeVar("E", contravariant=True)


class Observer[U, E](ABC):
    @abstractmethod
    def notify(
        self,
        notification: Notification[U, E],
    ) -> ContCert: ...
