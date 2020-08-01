from abc import ABC, abstractmethod

from rxbp.mixins.subscriptionmixin import SubscriptionMixin
from rxbp.selectors.baseandselectors import BaseAndSelectors


class IndexedSubscriptionMixin(SubscriptionMixin, ABC):

    @property
    @abstractmethod
    def index(self) -> BaseAndSelectors:
        ...
