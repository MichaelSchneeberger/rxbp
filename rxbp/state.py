from abc import ABC, abstractmethod
from dataclasses import replace
from threading import RLock
from typing import override

from dataclassabc import dataclassabc

from continuationmonad.typing import Scheduler, Trampoline, ContinuationCertificate


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
    def subscriber_count(self) -> dict: ...

    @property
    @abstractmethod
    def shared_weights(self) -> dict: ...

    @property
    @abstractmethod
    def connections(self) -> dict: ...

    @property
    @abstractmethod
    def connectable_observers(self) -> dict: ...

    @property
    @abstractmethod
    def certificate(self) -> ContinuationCertificate | None: ...


@dataclassabc(frozen=True)
class StateImpl(State):
    lock: RLock
    subscription_trampoline: Trampoline
    scheduler: Scheduler | None
    shared_observers: dict
    shared_weights: dict
    connections: dict
    connectable_observers: dict
    subscriber_count: dict
    certificate: ContinuationCertificate | None

    @override
    def copy(self, /, **changes):
        return replace(self, **changes)
    

def init_state(
    subscription_trampoline: Trampoline,
    scheduler: Scheduler | None = None,
    shared_observers: dict | None = None,
    shared_weights: dict | None = None,
    subscriber_count: dict | None = None,
    connections: dict | None = None,
    connectable_observers: dict | None = None,
    certificate: ContinuationCertificate | None = None,
):
    if subscriber_count is None:
        subscriber_count = {}

    if shared_observers is None:
        shared_observers = {}

    if shared_weights is None:
        shared_weights = {}

    if connections is None:
        connections = {}

    if connectable_observers is None:
        connectable_observers = {}

    return StateImpl(
        lock=RLock(),
        scheduler=scheduler, 
        subscription_trampoline=subscription_trampoline,
        subscriber_count=subscriber_count,
        shared_observers=shared_observers,
        shared_weights=shared_weights,
        connections=connections,
        connectable_observers=connectable_observers,
        certificate=certificate,
    )
