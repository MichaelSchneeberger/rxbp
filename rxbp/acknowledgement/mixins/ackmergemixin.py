from abc import ABC, abstractmethod

from rxbp.acknowledgement.ack import Ack


class AckMergeMixin(ABC):
    @abstractmethod
    def merge(self, other: Ack):
        ...
