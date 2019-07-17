from abc import ABC, abstractmethod

from rxbp.ack.ackbase import AckBase


class AckMixin(ABC):
    @abstractmethod
    def merge(self, other: AckBase):
        ...