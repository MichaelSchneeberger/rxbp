from dataclasses import dataclass, replace

from rxbp.flowabletree.observer import Observer


@dataclass
class SubscribeArgs[U]:
    observer: Observer[U]
    weight: int

    def copy(self, /, **changes):
        return replace(self, **changes)


def init_subscribe_args[U](
    observer: Observer[U],
    weight: int,
):
    return SubscribeArgs(
        observer=observer,
        weight=weight,
    )
