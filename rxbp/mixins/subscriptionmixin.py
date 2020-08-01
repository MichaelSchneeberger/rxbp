from abc import ABC, abstractmethod

from rxbp.mixins.copymixin import CopyMixin
from rxbp.observable import Observable


class SubscriptionMixin(CopyMixin, ABC):

    @property
    @abstractmethod
    def observable(self) -> Observable:
        ...
