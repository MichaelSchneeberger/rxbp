from dataclasses import replace

from dataclass_abc import dataclass_abc

from rxbp.multicast.multicastobserver import MultiCastObserver
from rxbp.multicast.multicastobserverinfo import MultiCastObserverInfo


@dataclass_abc
class MultiCastObserverInfoImpl(MultiCastObserverInfo):
    observer: MultiCastObserver

    def copy(self, observer: MultiCastObserver):
        return replace(self, observer=observer)
