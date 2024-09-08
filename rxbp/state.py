from abc import ABC, abstractmethod
from threading import RLock

from dataclassabc import dataclassabc

from continuationmonad.typing import Scheduler, Trampoline


class State(ABC):
    @property
    @abstractmethod
    def lock(self) -> RLock:
        ...

    @property
    @abstractmethod
    def subscription_trampoline(self) -> Trampoline:
        """
        Scheduler is propagated from downstream to upstream
        """

    @property
    @abstractmethod
    def scheduler(self) -> Scheduler | None:
        """
        Scheduler is propagated from downstream to upstream
        """

    @property
    @abstractmethod
    def shared_observables(self) -> dict:
        """
        Remembers shared flowables
        """


@dataclassabc(frozen=True)
class StateImpl(State):
    lock: RLock
    subscription_trampoline: Trampoline
    scheduler: Scheduler | None
    shared_observables: dict


def init_state(
    subscription_trampoline: Trampoline,
    scheduler: Scheduler | None = None,
    shared_observables: dict | None = None,
):
    if shared_observables is None:
        shared_observables = {}

    return StateImpl(
        scheduler=scheduler, 
        subscription_trampoline=subscription_trampoline,
        shared_observables=shared_observables,
        lock=RLock()
    )
