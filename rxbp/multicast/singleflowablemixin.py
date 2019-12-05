from abc import abstractmethod, ABC

from rxbp.flowable import Flowable


class SingleFlowableMixin(ABC):
    @abstractmethod
    def get_single_flowable(self) -> Flowable:
        ...