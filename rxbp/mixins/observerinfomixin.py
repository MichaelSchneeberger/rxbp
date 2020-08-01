from abc import ABC, abstractmethod

from rxbp.mixins.copymixin import CopyMixin
from rxbp.observer import Observer


class ObserverInfoMixin(CopyMixin, ABC):
    """
    downstream information provided when observing an observable
    """

    @property
    @abstractmethod
    def observer(self) -> Observer:
        ...

    @property
    @abstractmethod
    def is_volatile(self) -> bool:
        """
        has an effect on shared Flowables. If set to True, then a shared Flowable
        completes once there is no more non-volatile observers
        """

        ...
