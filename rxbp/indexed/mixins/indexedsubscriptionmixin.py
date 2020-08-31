from abc import ABC, abstractmethod

from rxbp.mixins.subscriptionmixin import SubscriptionMixin
from rxbp.indexed.selectors.flowablebaseandselectors import FlowableBaseAndSelectors


class IndexedSubscriptionMixin(SubscriptionMixin, ABC):

    @property
    @abstractmethod
    def index(self) -> FlowableBaseAndSelectors:
        ...
