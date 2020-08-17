from abc import ABC, abstractmethod

from rxbp.mixins.subscriptionmixin import SubscriptionMixin
from rxbp.subscriber import Subscriber


class FlowableMixin(ABC):
    @abstractmethod
    def unsafe_subscribe(self, subscriber: Subscriber) -> SubscriptionMixin:
        ...
