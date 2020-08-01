from abc import ABC, abstractmethod

from rxbp.mixins.observablemixin import ObservableMixin
from rxbp.typing import ValueType


# todo: delete
class SubscriberOpMixin(ABC):
    @abstractmethod
    def return_value(self, val: ValueType) -> ObservableMixin:
        ...
