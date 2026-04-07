from __future__ import annotations
from dataclasses import dataclass, replace

from contcert.typing import Scheduler

from rxbp.flowabletree.observer import Observer


@dataclass
class SubscribeArgs[U]:
    """
    Summarizes arguments of the ``unsafe_subscribe` method of a FlowableNode.
    """

    # downstream observer
    observer: Observer[U]

    # virtual multiplicity of a task execution
    virtual_count: int
    context: int

    # default scheduler when no explicit scheduler is provided
    scheduler: Scheduler

    def copy[V](
        self, /,
        observer: Observer[V] | None = None,
        virtual_count: int | None = None,
        scheduler: Scheduler | None = None,
    ) -> SubscribeArgs[V]:
        def gen_args():
            if observer is not None:
                yield 'observer', observer
            if virtual_count is not None:
                yield 'virtual_count', virtual_count
            if scheduler is not None:
                yield 'scheduler', scheduler

        args = dict(gen_args())
        return replace(self, **args)


def init_subscribe_args[U](
    observer: Observer[U],
    virtual_count: int,
    context: int,
    scheduler: Scheduler,
) -> SubscribeArgs[U]:
    return SubscribeArgs(
        observer=observer,
        virtual_count=virtual_count,
        context=context,
        scheduler=scheduler,
    )

