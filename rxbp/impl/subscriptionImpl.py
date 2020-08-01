from dataclasses import replace

from dataclass_abc import dataclass_abc

from rxbp.observable import Observable
from rxbp.subscription import Subscription


@dataclass_abc
class SubscriptionImpl(Subscription):
    observable: Observable

    def copy(self, **kwargs):
        return replace(self, **kwargs)
