from abc import ABC, abstractmethod


class IsHotMultiCastMixin(ABC):
    @property
    @abstractmethod
    def nested_layer(self) -> int:
        ...

    @property
    @abstractmethod
    def is_hot(self) -> bool:
        ...
