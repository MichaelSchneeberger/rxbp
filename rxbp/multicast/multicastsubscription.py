import dataclasses
from abc import ABC, abstractmethod

from rxbp.multicast.multicastobservable import MultiCastObservable


class MultiCastSubscription(ABC):
    @property
    @abstractmethod
    def observable(self) -> MultiCastObservable:
        pass

    def copy(self, **kwargs):
        return dataclasses.replace(self, **kwargs)

