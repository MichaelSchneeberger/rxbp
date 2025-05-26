from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import replace
from typing import override

from dataclassabc import dataclassabc

import continuationmonad
from continuationmonad.typing import Trampoline


class State(ABC):
    # Settings
    ##########

    @property
    @abstractmethod
    def raise_immediately(self) -> bool:
        """
        If set to False, an exception raised within a user provided function (e.g., func in flat_map),
        the exception is caught and propagated using the Observble interface `observer.on_error`.
        Otherwise, the exception is raised immediately.
        """

    # Sending items
    ###############

    @property
    @abstractmethod
    def subscription_trampoline(self) -> Trampoline:
        """
        Trampoline ensures that all Flowable nodes are subscribe before first items are sent
        """

    # Shared Flowable nodes
    #######################

    @property
    @abstractmethod
    def shared_observers(self) -> dict:
        """
        Dictionary containing all shared Flowable nodes that are encountered during the subscription process.
        It maps the shared Flowable node to its observer created when first subscribing.
        When subscribing multiple times to the same Flowable node, the observer is used that is stored in the
        dictionary.

        This dictionary is modified during the subscription of a Flowable.
        """

    @property
    @abstractmethod
    def shared_subscribe_count(self) -> dict:
        """
        To properly assign the weight attribute in case of shared Flowables, the number of observer
        needs to be evaluated first.

        This dictionary is created during the discovering phase before subscribing to a Flowable.
        """

    @property
    @abstractmethod
    def shared_weights(self) -> dict:
        """
        The downstream weight of the Continuation Certificate of all shared Flowable nodes.

        This dictionary is created during the weight assigning phase before subscribing to a Flowable.
        """

    # Connectable Flowable node
    ###########################

    @property
    @abstractmethod
    def connections(self) -> dict:
        """
        Maps connectable Flowable nodes to its source.

        This dictionary needs to be provided when subscribing to a Flowable.
        """

    @property
    @abstractmethod
    def discovered_connectables(self) -> list:
        """
        A list that is created during the discovering phase before subscribing to a Flowable.
        It is needed to assign the weights of the specified source of a connectable Flowable node.
        """

    @property
    @abstractmethod
    def discovered_subscriptions(self) -> list:
        """
        A list that contains the subscriptions of all connectable Flowable nodes encountered during
        the current subription.
        """

    @abstractmethod
    def copy(self, /, **changes) -> State: ...


@dataclassabc(frozen=True)
class StateImpl(State):
    subscription_trampoline: Trampoline
    raise_immediately: bool
    shared_observers: dict
    shared_subscribe_count: dict
    shared_weights: dict
    connections: dict
    discovered_connectables: list
    discovered_subscriptions: list

    @override
    def copy(self, /, **changes):
        return replace(self, **changes)


def init_state(
    subscription_trampoline: Trampoline | None = None,
    raise_immediately: bool | None = None,
    shared_observers: dict | None = None,
    shared_subscribe_count: dict | None = None,
    shared_weights: dict | None = None,
    connections: dict | None = None,
    discovered_connectables: list | None = None,
    discovered_subscriptions: list | None = None,
):
    if subscription_trampoline is None:
        subscription_trampoline = continuationmonad.init_trampoline()

    if raise_immediately is None:
        raise_immediately = True

    if shared_observers is None:
        shared_observers = {}

    if shared_subscribe_count is None:
        shared_subscribe_count = {}

    if shared_weights is None:
        shared_weights = {}

    if connections is None:
        connections = {}

    if discovered_connectables is None:
        discovered_connectables = []

    if discovered_subscriptions is None:
        discovered_subscriptions = []

    return StateImpl(
        subscription_trampoline=subscription_trampoline,
        raise_immediately=raise_immediately,
        shared_observers=shared_observers,
        shared_subscribe_count=shared_subscribe_count,
        shared_weights=shared_weights,
        connections=connections,
        discovered_connectables=discovered_connectables,
        discovered_subscriptions=discovered_subscriptions,
    )
