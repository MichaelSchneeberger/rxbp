import dataclasses
from dataclasses import dataclass

from dataclass_abc import dataclass_abc

from rxbp.multicast.mixins.multicastobservablemixin import MultiCastObservableMixin
from rxbp.multicast.multicastsubscription import MultiCastSubscription


@dataclass_abc
class MultiCastSubscriptionImpl(MultiCastSubscription):
    observable: MultiCastObservableMixin

    def copy(self, **kwargs):
        return dataclasses.replace(self, **kwargs)
