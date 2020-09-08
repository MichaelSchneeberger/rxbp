from abc import ABC

from rxbp.multicast.mixins.multicastsubscribermixin import MultiCastSubscriberMixin


class MultiCastSubscriber(MultiCastSubscriberMixin, ABC):
    pass
