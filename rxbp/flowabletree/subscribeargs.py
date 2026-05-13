from __future__ import annotations
from dataclasses import dataclass, replace

from contcert.typing import Scheduler



@dataclass
class SubscribeArgs:
    """
    Summarizes arguments of the ``unsafe_subscribe` method of a FlowableNode.
    """

    # virtual multiplicity of a task execution
    virtual_count: int
    async_index: int

    # default scheduler when no explicit scheduler is provided
    scheduler: Scheduler

    def copy[V](
        self, /,
        virtual_count: int | None = None,
        context: int | None = None,
        scheduler: Scheduler | None = None,
    ) -> SubscribeArgs:
        def gen_args():
            if virtual_count is not None:
                yield 'virtual_count', virtual_count
            if context is not None:
                yield 'context', context
            if scheduler is not None:
                yield 'scheduler', scheduler

        args = dict(gen_args())
        return replace(self, **args)


def init_subscribe_args(
    virtual_count: int,
    context: int,
    scheduler: Scheduler,
) -> SubscribeArgs:
    return SubscribeArgs(
        virtual_count=virtual_count,
        async_index=context,
        scheduler=scheduler,
    )

