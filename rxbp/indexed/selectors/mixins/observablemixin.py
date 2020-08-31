from abc import ABC, abstractmethod

from rxbp.observable import Observable


class ObservableMixin(ABC):
    @property
    @abstractmethod
    def observable(self) -> Observable:
        ...
