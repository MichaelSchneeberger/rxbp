from abc import ABC, abstractmethod

from rxbp.multicast.multicastsubscriber import MultiCastSubscriber
from rxbp.multicast.multicastsubscription import MultiCastSubscription


class MultiCastMixin(ABC):
    @abstractmethod
    def unsafe_subscribe(
            self,
            subscriber: MultiCastSubscriber,
    ) -> MultiCastSubscription:
        ...
