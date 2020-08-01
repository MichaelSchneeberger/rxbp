from dataclasses import dataclass

from rxbp.multicast.mixins.multicastobservermixin import MultiCastObserverMixin


@dataclass
class MultiCastObserverInfo:
    observer: MultiCastObserverMixin

    def copy(self, observer: MultiCastObserverMixin):
        return MultiCastObserverInfo(observer=observer)
