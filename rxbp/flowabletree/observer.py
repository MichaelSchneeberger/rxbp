from abc import ABC, abstractmethod

from continuationmonad.typing import ContinuationCertificate, ContinuationMonad


class Observer[V](ABC):
    @abstractmethod
    def on_next(self, value: V) -> ContinuationMonad[None]: ...

    @abstractmethod
    def on_next_and_complete(self, value: V) -> ContinuationMonad[ContinuationCertificate]: ...

    @abstractmethod
    def on_completed(self) -> ContinuationMonad[ContinuationCertificate]: ...

    @abstractmethod
    def on_error(self, exception: Exception) -> ContinuationMonad[ContinuationCertificate]: ...
