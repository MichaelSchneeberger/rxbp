from dataclasses import dataclass, replace

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

    def copy(
        self, /,
        observer: Observer | None = None,
        weight: int | None = None,
        scheduler: Scheduler | None = None,
    ):
        def gen_args():
            if observer is not None:
                yield 'observer', observer
            if weight is not None:
                yield 'weight', weight
            if scheduler is not None:
                yield 'scheduler', scheduler

        args = dict(gen_args())
        return replace(self, **args)


def init_subscribe_args[U](
    observer: Observer[U],
    weight: int,
    scheduler: Scheduler,
):
    return SubscribeArgs(
        observer=observer,
        weight=weight,
        scheduler=scheduler,
    )
