from abc import ABC, abstractmethod

from continuationmonad.abc import Cancellation
from continuationmonad.typing import ContinuationCertificate


class Cancellable(ABC):
    @abstractmethod
    def cancel(self, certificate: ContinuationCertificate):
        ...

class CancellationState(Cancellation, Cancellable):
    def __init__(self):
        self._certificate = None

    def cancel(self, certificate: ContinuationCertificate):
        self._certificate = certificate

    def is_cancelled(self) -> ContinuationCertificate | None:
        return self._certificate


def init_cancellation_state():
    return CancellationState()

