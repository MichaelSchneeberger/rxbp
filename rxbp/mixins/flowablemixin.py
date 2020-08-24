from abc import ABC, abstractmethod

from rxbp.subscriber import Subscriber
from rxbp.subscription import Subscription


class FlowableMixin(ABC):
    @abstractmethod
    def unsafe_subscribe(self, subscriber: Subscriber) -> Subscription:
        ...
