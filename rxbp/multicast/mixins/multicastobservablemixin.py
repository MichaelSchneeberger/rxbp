from abc import abstractmethod, ABC

import rx

from rxbp.multicast.multicastobserverinfo import MultiCastObserverInfo


class MultiCastObservableMixin(ABC):
    @abstractmethod
    def observe(self, observer_info: MultiCastObserverInfo) -> rx.typing.Disposable:
        ...
