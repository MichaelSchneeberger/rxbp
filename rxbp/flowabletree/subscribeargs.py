

from dataclasses import dataclass, replace

from rxbp.flowabletree.observer import Observer


@dataclass
class SubscribeArgs[V]:
    observer: Observer[V]
    weight: int

    def copy(self, /, **changes):
        return replace(self, **changes)
