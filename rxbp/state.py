from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Any, override


from contcert.typing import Trampoline
import rxsingle

from rxbp.flowabletree.observer import Observer
from rxbp.flowabletree.operations.share.flowable import ShareFlowable


@dataclass
class State(ABC):
    # Settings
    ##########

    """
    If set to False, an exception raised within a user provided function (e.g., func in flat_map),
    the exception is caught and propagated using the Observble interface `observer.on_error`.
    Otherwise, the exception is raised immediately.
    """
    raise_immediately: bool

    # Sending items
    ###############

    """
    Trampoline ensures that all Flowable nodes are subscribe before first items are sent
    """
    subscription_trampoline: Trampoline

    # Shared Flowable nodes
    #######################

    """
    Dictionary containing all shared Flowable nodes that are encountered during the subscription process.
    It maps the shared Flowable node to its observer created when first subscribing.
    When subscribing multiple times to the same Flowable node, the observer is used that is stored in the
    dictionary.

    This dictionary is modified during the subscription of a Flowable.
    """
    shared_observers: dict[ShareFlowable, Observer[object]]

    """
    To properly assign the virtual_count attribute in case of shared Flowables, the number of observer
    needs to be evaluated first.

    This dictionary is created during the discovering phase before subscribing to a Flowable.
    """
    shared_subscribe_count: dict[ShareFlowable, int]

    """
    The downstream virtual_count of the Continuation Certificate of all shared Flowable nodes.

    This dictionary is created during the virtual_count assigning phase before subscribing to a Flowable.
    """
    shared_virtual_count: dict[ShareFlowable, int]

    # Connectable Flowable node
    ###########################

    """
    Maps connectable Flowable nodes to its source.

    This dictionary needs to be provided when subscribing to a Flowable.
    """
    connections: dict

    """
    A list that is created during the discovering phase before subscribing to a Flowable.
    It is needed to assign the virtual_counts of the specified source of a connectable Flowable node.
    """
    discovered_connectables: list

    """
    A list that contains the subscriptions of all connectable Flowable nodes encountered during
    the current subription.
    """
    discovered_subscriptions: list

    def copy(self, /, **changes):
        return replace(self, **changes)


def init_state(
    subscription_trampoline: Trampoline | None = None,
    raise_immediately: bool | None = None,
    shared_observers: dict | None = None,
    shared_subscribe_count: dict | None = None,
    shared_virtual_counts: dict | None = None,
    connections: dict | None = None,
    discovered_connectables: list | None = None,
    discovered_subscriptions: list | None = None,
):
    if subscription_trampoline is None:
        subscription_trampoline = contcert.init_trampoline()

    if raise_immediately is None:
        raise_immediately = True

    if shared_observers is None:
        shared_observers = {}

    if shared_subscribe_count is None:
        shared_subscribe_count = {}

    if shared_virtual_counts is None:
        shared_virtual_counts = {}

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
        shared_virtual_counts=shared_virtual_counts,
        connections=connections,
        discovered_connectables=discovered_connectables,
        discovered_subscriptions=discovered_subscriptions,
    )
