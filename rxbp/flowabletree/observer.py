from abc import ABC, abstractmethod

from contcert.typing import ContCert, Trampoline
from rxsingle.typing import Observer as RxSingleObserver


class Observer[U](ABC):
    @abstractmethod
    def on_next(
        self, 
        trampoline: Trampoline,
        item: U, 
        observer: RxSingleObserver[None],
    ) -> ContCert: ...

    @abstractmethod
    def on_next_and_complete(
        self,
        trampoline: Trampoline,
        item: U,
    ) -> ContCert: ...

    @abstractmethod
    def on_completed(self) -> ContCert: ...

    @abstractmethod
    def on_error(
        self,
        exception: Exception,
    ) -> ContCert: ...

