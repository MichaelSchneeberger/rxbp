import dataclasses

from dataclass_abc import dataclass_abc

from rxbp.multicast.multicastobservable import MultiCastObservable
from rxbp.multicast.multicastsubscription import MultiCastSubscription


@dataclass_abc
class MultiCastSubscriptionImpl(MultiCastSubscription):
    observable: MultiCastObservable

    def copy(self, **kwargs):
        return dataclasses.replace(self, **kwargs)
