import dataclasses
from abc import ABC, abstractmethod

from rxbp.multicast.mixins.multicastobservablemixin import MultiCastObservableMixin


class MultiCastSubscription(ABC):
    @property
    @abstractmethod
    def observable(self) -> MultiCastObservableMixin:
        pass

    def copy(self, **kwargs):
        return dataclasses.replace(self, **kwargs)

