from abc import ABC, abstractmethod


class CopyMixin(ABC):
    @abstractmethod
    def copy(self, **kwargs):
        ...
