

from dataclasses import dataclass, replace

from rxbp.flowabletree.observer import Observer


@dataclass
class SubscribeArgs[V]:
    observer: Observer[V]
    schedule_weight: int

    def copy(self, /, **changes):
        return replace(self, **changes)
