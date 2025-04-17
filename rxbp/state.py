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
    def shared_observers(self) -> dict:
        """
        Remembers shared flowables
        """

    @property
    @abstractmethod
    def shared_weights(self) -> dict: ...
    


@dataclassabc(frozen=True)
class StateImpl(State):
    lock: RLock
    subscription_trampoline: Trampoline
    scheduler: Scheduler | None
    shared_observers: dict
    shared_weights: dict


def init_state(
    subscription_trampoline: Trampoline,
    scheduler: Scheduler | None = None,
    shared_observers: dict | None = None,
    shared_weights: dict | None = None,
):
    if shared_observers is None:
        shared_observers = {}

    if shared_weights is None:
        shared_weights = {}

    return StateImpl(
        lock=RLock(),
        scheduler=scheduler, 
        subscription_trampoline=subscription_trampoline,
        shared_observers=shared_observers,
        shared_weights=shared_weights,
    )
