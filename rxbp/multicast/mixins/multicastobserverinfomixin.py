from abc import ABC, abstractmethod

from rxbp.multicast.multicastobserver import MultiCastObserver


class MultiCastObserverInfoMixin(ABC):
    @property
    @abstractmethod
    def observer(self) -> MultiCastObserver:
        ...
