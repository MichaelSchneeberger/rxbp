from abc import ABC, abstractmethod

from rxbp.mixins.flowablemixin import FlowableMixin


class IsHotFlowableMixin(FlowableMixin, ABC):
    pass
    # @property
    # @abstractmethod
    # def is_hot(self) -> bool:
    #     ...
