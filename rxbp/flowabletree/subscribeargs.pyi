from __future__ import annotations

from dataclasses import dataclass
from typing import overload

from continuationmonad.typing import Scheduler

from rxbp.flowabletree.observer import Observer


@dataclass
class SubscribeArgs[U]:
    """
    Summarizes arguments of the ``unsafe_subscribe` method of a FlowableNode.
    """

    # downstream observer
    observer: Observer[U]

    # virtual multiplicity of a task execution
    weight: int

    # default scheduler when no explicit scheduler is provided
    scheduler: Scheduler

    @overload
    def copy[V](
        self, /,
        observer: Observer[V],
        weight: int | None = None,
        scheduler: Scheduler | None = None,
    ) -> SubscribeArgs[V]: ...
    @overload
    def copy(
        self, /,
        weight: int | None = None,
        scheduler: Scheduler | None = None,
    ) -> SubscribeArgs[U]: ...


def init_subscribe_args[U](
    observer: Observer[U],
    weight: int,
    scheduler: Scheduler,
) -> SubscribeArgs[U]: ...
