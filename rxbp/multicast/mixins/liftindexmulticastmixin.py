from abc import ABC, abstractmethod

from rxbp.multicast.mixins.multicastmixin import MultiCastMixin


class LiftIndexMultiCastMixin(MultiCastMixin, ABC):
    @property
    @abstractmethod
    def lift_index(self) -> int:
        ...
