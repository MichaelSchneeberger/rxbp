from abc import ABC, abstractmethod

from rxbp.ack.mixins.ackmixin import AckMixin


class AckMergeMixin(ABC):
    @abstractmethod
    def merge(self, other: AckMixin):
        ...
