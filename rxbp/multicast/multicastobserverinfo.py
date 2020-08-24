from dataclasses import dataclass

from rxbp.multicast.multicastobserver import MultiCastObserver


@dataclass
class MultiCastObserverInfo:
    observer: MultiCastObserver

    def copy(self, observer: MultiCastObserver):
        return MultiCastObserverInfo(observer=observer)
