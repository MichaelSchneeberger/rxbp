from abc import abstractmethod, ABC

from rx.disposable import Disposable

from rxbp.multicast.multicastobserverinfo import MultiCastObserverInfo


class MultiCastObservableMixin(ABC):
    @abstractmethod
    def observe(self, observer: MultiCastObserverInfo) -> Disposable:
        ...
