from abc import ABC, abstractmethod
from dataclasses import replace
from threading import RLock
from typing import override

import continuationmonad
from dataclassabc import dataclassabc

from continuationmonad.typing import Scheduler, Trampoline, ContinuationCertificate


class State(ABC):
    @property
    @abstractmethod
    def lock(self) -> RLock:        # Remove?
        ...

    @property
    @abstractmethod
    def subscription_trampoline(self) -> Trampoline:
        """
        Scheduler is propagated from downstream to upstream
        """

    @property
    @abstractmethod
    def scheduler(self) -> Scheduler:
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
    def shared_subscribe_count(self) -> dict: ...

    @property
    @abstractmethod
    def shared_weights(self) -> dict: ...

    @property
    @abstractmethod
    def connections(self) -> dict: ...

    @property
    @abstractmethod
    def discovered_connectables(self) -> list: ...

    @property
    @abstractmethod
    def discovered_subscriptions(self) -> list: ...

    # @property
    # @abstractmethod
    # def connectable_observers(self) -> dict: ...

    @property
    @abstractmethod
    def certificate(self) -> ContinuationCertificate | None: ...

    @property
    @abstractmethod
    def raise_immediately(self) -> bool: ...


@dataclassabc(frozen=True)
class StateImpl(State):
    lock: RLock
    subscription_trampoline: Trampoline
    scheduler: Scheduler | None
    shared_observers: dict
    shared_subscribe_count: dict
    discovered_connectables: list
    discovered_subscriptions: list
    shared_weights: dict
    connections: dict
    # connectable_observers: dict
    certificate: ContinuationCertificate | None
    raise_immediately: bool

    @override
    def copy(self, /, **changes):
        return replace(self, **changes)
    

def init_state(
    scheduler: Scheduler,
    subscription_trampoline: Trampoline | None = None,
    shared_observers: dict | None = None,
    shared_subscribe_count: dict | None = None,
    shared_weights: dict | None = None,
    connections: dict | None = None,
    discovered_connectables: list | None = None,
    discovered_subscriptions: list | None = None,
    # connectable_observers: dict | None = None,
    certificate: ContinuationCertificate | None = None,
    raise_immediately: bool | None = None,
):
    if subscription_trampoline is None:
        subscription_trampoline = continuationmonad.init_trampoline()

    if shared_subscribe_count is None:
        shared_subscribe_count = {}

    if shared_observers is None:
        shared_observers = {}

    if shared_weights is None:
        shared_weights = {}

    if connections is None:
        connections = {}

    if discovered_subscriptions is None:
        discovered_subscriptions = []

    if discovered_connectables is None:
        discovered_connectables = []

    if raise_immediately is None:
        raise_immediately = True

    return StateImpl(
        lock=RLock(),
        scheduler=scheduler, 
        subscription_trampoline=subscription_trampoline,
        shared_subscribe_count=shared_subscribe_count,
        shared_observers=shared_observers,
        shared_weights=shared_weights,
        connections=connections,
        discovered_connectables=discovered_connectables,
        discovered_subscriptions=discovered_subscriptions,
        certificate=certificate,
        raise_immediately=raise_immediately,
    )
