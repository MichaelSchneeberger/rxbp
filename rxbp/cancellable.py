from abc import ABC, abstractmethod

from contcert.abc import Cancellation
from contcert.typing import ContCert


class Cancellable(ABC):
    @abstractmethod
    def cancel(self, certificate: ContCert):
        ...

class CancellationState(Cancellation, Cancellable):
    def __init__(self):
        self._certificate = None

    def cancel(self, certificate: ContCert):
        self._certificate = certificate

    def is_cancelled(self) -> ContCert | None:
        return self._certificate


def init_cancellation_state():
    return CancellationState()

