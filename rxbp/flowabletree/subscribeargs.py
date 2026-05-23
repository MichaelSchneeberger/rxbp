from __future__ import annotations
from abc import ABC, abstractmethod
from dataclasses import dataclass, replace
from typing import Any, Self

from contcert.typing import Scheduler

@dataclass
class SubscribeArgs(ABC):
    """
    Summarizes arguments of the ``unsafe_subscribe` method of a FlowableNode.
    """

    # virtual multiplicity of a task execution
    @property
    @abstractmethod
    def p_virtual_count(self) -> int: ...

    @property
    @abstractmethod
    def p_async_index(self) -> int: ...

    # default scheduler when no explicit scheduler is provided
    @property
    @abstractmethod
    def p_scheduler(self) -> Scheduler: ...

    @abstractmethod
    def update_basics[V](
        self, /,
        virtual_count: int | None = None,
        async_index: int | None = None,
        scheduler: Scheduler | None = None,
    ) -> Self: ...

    # def copy[V](
    #     self, /,
    #     virtual_count: int | None = None,
    #     async_index: int | None = None,
    #     scheduler: Scheduler | None = None,
    #     async_share_observers: dict[AsyncShareSubject[Any, Any, Any], AsyncShareObserver[Any, Any]] | None = None,  # pyright: ignore[reportExplicitAny]
    # ) -> SubscribeArgs:
    #     def gen_args():
    #         if virtual_count is not None:
    #             yield 'virtual_count', virtual_count
    #         if async_index is not None:
    #             yield 'async_index', async_index
    #         if scheduler is not None:
    #             yield 'scheduler', scheduler
    #         if async_share_observers is not None:
    #             yield 'async_share_observers', async_share_observers
    #
    #     args = dict(gen_args())
    #     return replace(self, **args)


# def init_subscribe_args(
#     virtual_count: int,
#     async_index: int,
#     scheduler: Scheduler,
#     async_share_observers: dict[AsyncShareSubject[Any, Any, Any], AsyncShareObserver[Any, Any]],  # pyright: ignore[reportExplicitAny]
# ) -> SubscribeArgs:
#     return SubscribeArgs(
#         virtual_count=virtual_count,
#         async_index=async_index,
#         scheduler=scheduler,
#         async_share_observers=async_share_observers,
#     )

