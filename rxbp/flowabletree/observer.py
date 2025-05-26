from abc import ABC, abstractmethod

from continuationmonad.typing import ContinuationCertificate, ContinuationMonad


class Observer[U](ABC):
    @abstractmethod
    def on_next(
        self, item: U,
    ) -> ContinuationMonad[None]: ...

    @abstractmethod
    def on_next_and_complete(
        self, item: U
    ) -> ContinuationMonad[ContinuationCertificate]: ...

    @abstractmethod
    def on_completed(self) -> ContinuationMonad[ContinuationCertificate]: ...

    @abstractmethod
    def on_error(
        self, exception: Exception
    ) -> ContinuationMonad[ContinuationCertificate]: ...
