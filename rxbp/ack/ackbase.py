from abc import ABC, abstractmethod

from rx.disposable import Disposable
from rxbp.ack.single import Single


class AckBase(ABC):

    @abstractmethod
    def subscribe(self, single: Single) -> Disposable:
        ...
