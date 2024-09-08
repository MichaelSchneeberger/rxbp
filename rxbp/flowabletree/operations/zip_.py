from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, replace
from itertools import accumulate
from threading import RLock
from typing import Any, Callable

from dataclassabc import dataclassabc
from donotation import do

import continuationmonad
from continuationmonad.typing import (
    Cancellable,
    ContinuationCertificate,
    DeferredSubscription,
)

from rxbp.cancellables.compositecancellable import CompositeCancellable
from rxbp.utils.lockmixin import LockMixin
from rxbp.flowabletree.data.observer import Observer
from rxbp.flowabletree.nodes import MultiChildrenFlowableNode, FlowableNode
from rxbp.flowabletree.data.observeresult import ObserveResult
from rxbp.state import State


type UpstreamID = int


class DownstreamAction(ABC):
    pass


@dataclass
class DownstreamPending(DownstreamAction):
    n_requested: int
    certificates: list[ContinuationCertificate]
    pending: tuple[OnNextAction, ...]
    actions: list[OnNextAction]


class DownstreamOnNextAction(DownstreamAction):
    @property
    @abstractmethod
    def actions(self) -> dict[UpstreamID, OnNextAction]: ...


@dataclassabc(frozen=True)
class DownstreamOnNext(DownstreamOnNextAction):
    actions: dict[UpstreamID, OnNextAction]


@dataclassabc(frozen=True)
class DownstreamOnNextAndComplete(DownstreamOnNextAction):
    actions: dict[UpstreamID, OnNextAction]


class DownstreamTerminated(DownstreamAction):
    @property
    @abstractmethod
    def certificates(self) -> list[ContinuationCertificate]: ...


@dataclassabc(frozen=True)
class DownstreamOnError(DownstreamTerminated):
    exception: Exception
    certificates: list[ContinuationCertificate]


@dataclassabc(frozen=True)
class DownstreamOnComplete(DownstreamTerminated):
    certificates: list[ContinuationCertificate]


@dataclassabc(frozen=True)
class DownstreamHasTerminated(DownstreamTerminated):
    certificates: list[ContinuationCertificate]


class UpstreamActionNode(ABC):
    @abstractmethod
    def traverse(self) -> DownstreamAction: ...


@dataclass
class SubscribeState(UpstreamActionNode):
    def traverse(self) -> DownstreamAction:
        raise Exception("not good")


@dataclass
class InitialAction(UpstreamActionNode):
    n_requested: int
    certificates: list[ContinuationCertificate]
    pending: tuple[OnNextAction, ...]

    def traverse(self) -> DownstreamAction:
        return DownstreamPending(
            n_requested=self.n_requested,
            certificates=self.certificates,
            pending=self.pending,
            actions=[],
        )


class SingleChildNode(UpstreamActionNode):
    @property
    @abstractmethod
    def child(self) -> UpstreamActionNode: ...


class OnNextAction[U](SingleChildNode):
    @property
    @abstractmethod
    def id(self) -> UpstreamID: ...

    @property
    @abstractmethod
    def value(self) -> U: ...

    @abstractmethod
    def copy(self, value) -> OnNextAction: ...

    def traverse(self) -> DownstreamAction:
        p_state = self.child.traverse()
        match p_state:
            case DownstreamPending(
                actions=actions, n_requested=n_requested, pending=pending
            ):
                if len(actions) == n_requested - 1:
                    n_actions = {
                        action.id: action
                        for action in pending + tuple(actions) + (self,)
                    }
                    if all(
                        isinstance(action, OnNextAndComplete) for action in n_actions
                    ):
                        return DownstreamOnNextAndComplete(actions=n_actions)
                    else:
                        return DownstreamOnNext(actions=n_actions)
                else:
                    p_state.actions.append(self)
                    return p_state
            case _:
                return p_state


@dataclassabc
class OnNext[U](OnNextAction[U]):
    id: UpstreamID
    value: U
    child: UpstreamActionNode
    subscription: DeferredSubscription

    def copy(self, value):
        return replace(self, value=value)


@dataclassabc
class OnNextAndComplete[U](OnNextAction[U]):
    id: UpstreamID
    value: U
    child: UpstreamActionNode

    def copy(self, value):
        return replace(self, value=value)


@dataclassabc
class OnComplete(SingleChildNode):
    child: UpstreamActionNode

    def traverse(self) -> DownstreamAction:
        p_state = self.child.traverse()
        match p_state:
            case DownstreamPending(certificates=certificates):
                return DownstreamOnComplete(certificates=certificates)
            case DownstreamTerminated(certificates=certificates):
                return DownstreamHasTerminated(certificates=certificates)
            case _:
                raise Exception(f"Unknown state {p_state}")


@dataclassabc
class OnError(SingleChildNode):
    child: UpstreamActionNode
    exception: Exception

    def traverse(self) -> DownstreamAction:
        p_state = self.child.traverse()
        match p_state:
            case DownstreamPending(certificates=certificates):
                return DownstreamOnError(
                    exception=self.exception, certificates=certificates
                )
            case DownstreamTerminated(certificates=certificates):
                return DownstreamHasTerminated(certificates=certificates)
            case _:
                raise Exception(f"Unknown state {p_state}")


@dataclassabc
class ZipSharedMemory(LockMixin):
    upstream: Observer
    zip_func: Callable[
        [tuple[UpstreamID, ...], tuple[Any]],
        tuple[tuple[Any, ...], tuple[tuple[UpstreamID, Any], ...]],
    ]
    node: UpstreamActionNode
    cancellables: tuple[Cancellable, ...]
    lock: RLock


@dataclass
class ZipObserver[V](Observer[V]):
    shared: ZipSharedMemory
    id: UpstreamID

    @do()
    def _on_next(self, action: UpstreamActionNode):
        zip_action = action.traverse()

        match zip_action:
            case (
                DownstreamTerminated(certificates=certificates)
                | DownstreamPending(certificates=certificates)
            ):
                with self.shared.lock:
                    certificate = certificates.pop()
                return continuationmonad.from_(certificate)

            case DownstreamOnNextAction(actions=actions):

                def gen_id_value_pairs():
                    for id, action in actions.items():
                        yield id, action.value

                ids, values = zip(*sorted(gen_id_value_pairs()))
                zipped_values, pending_values = self.shared.zip_func(ids, values)
                # print(zip_action)

                def gen_updated_nodes():
                    for id, value in pending_values:
                        yield id, actions[id].copy(value=value)

                pending = dict(gen_updated_nodes())

                complet_downstream = [False]

                def gen_subscriptions():
                    for id, action in actions.items():
                        if id not in pending:
                            match action:
                                case OnNext(subscription=subscription):
                                    yield subscription
                                case _:
                                    complet_downstream[0] = True

                subscriptions = tuple(gen_subscriptions())

                if complet_downstream[0]:
                    return self.shared.upstream.on_next_and_complete(zipped_values)

                match zip_action:
                    case DownstreamOnNextAndComplete():
                        return self.shared.upstream.on_next_and_complete(zipped_values)

                    case DownstreamOnNext(actions=actions):
                        _ = yield from self.shared.upstream.on_next(zipped_values)

                        # trampoline = yield from continuationmonad.get_trampoline()

                        certificate, *certificates = yield from continuationmonad.from_(
                            None
                        ).connect(subscriptions)

                        # def gen_certificates():
                        #     for subscription in subscriptions:
                        #         def request_next_item(subscription=subscription):
                        #             return subscription.on_next(trampoline, None)
                        #         yield trampoline.schedule(request_next_item)

                        # certificates = list(gen_certificates())
                        # certificate = certificates.pop()

                        self.shared.node = InitialAction(
                            certificates=certificates,
                            pending=tuple(pending.values()),
                            n_requested=len(
                                subscriptions,
                            ),
                        )

                        return continuationmonad.from_(certificate)
            case _:
                raise Exception()

    @do()
    def on_next(self, value: V):
        # wait for upstream subscription before continuing to simplify concurrency
        def on_next_ackowledgment(subscription: DeferredSubscription):
            node = OnNext(
                id=self.id,
                value=value,
                subscription=subscription,
                child=None,  # type: ignore
            )

            with self.shared.lock:
                node.child = self.shared.node
                self.shared.node = node

            return self._on_next(node)

        return continuationmonad.defer(on_next_ackowledgment)

    @do()
    def on_next_and_complete(
        self, value: V
    ):  # -> ContinuationMonad[Continuation | None]:
        node = OnNextAndComplete(
            id=self.id,
            value=value,
            child=None,  # type: ignore
        )

        with self.shared.lock:
            node.child = self.shared.node
            self.shared.node = node

        return self._on_next(node)

    def on_completed(self):  # -> ContinuationMonad[Continuation | None]:
        node = OnComplete(child=None)  # type: ignore

        with self.shared.lock:
            node.child = self.shared.node
            self.shared.node = node

        action = node.traverse()

        match action:
            case DownstreamOnComplete():
                return self.shared.upstream.on_completed()

            case DownstreamTerminated(certificates=certificates):
                with self.shared.lock:
                    certificate = certificates.pop()
                return continuationmonad.from_(certificate)

    def on_error(
        self, exception: Exception
    ):  # -> ContinuationMonad[Continuation | None]:
        node = OnError(
            child=None,  # type: ignore
            exception=exception,
        )

        with self.shared.lock:
            node.child = self.shared.node
            self.shared.node = node

        action = node.traverse()

        match action:
            case DownstreamOnError(exception=exception):
                return self.shared.upstream.on_error(exception)

            case DownstreamTerminated(certificates=certificates):
                with self.shared.lock:
                    certificate = certificates.pop()
                return continuationmonad.from_(certificate)


class Zip[V](MultiChildrenFlowableNode[V]):
    @do()
    def unsafe_subscribe(
        self, state: State, observer: Observer[V]
    ) -> tuple[State, ObserveResult]:
        def zip_func(ids: tuple[UpstreamID, ...], values: tuple[Any, ...]):
            return values, tuple()

        shared_state = ZipSharedMemory(
            lock=state.lock,
            upstream=observer,
            zip_func=zip_func,
            node=SubscribeState(),  # type: ignore
            cancellables=None,  # type: ignore
        )

        def acc_continuations(
            acc: tuple[State, list[ContinuationCertificate], list[Cancellable]],
            value: tuple[int, FlowableNode],
        ):
            state, certificates, cancellables = acc
            id, child = value

            observer = ZipObserver(
                shared=shared_state,
                id=id,
            )

            n_state, n_result = child.unsafe_subscribe(state, observer)

            if n_result.certificate:
                certificates.append(n_result.certificate)

            cancellables.append(n_result.cancellable)

            return n_state, certificates, cancellables

        *_, (n_state, certificates, cancellables_list) = accumulate(
            func=acc_continuations,
            iterable=enumerate(self.children),
            initial=(state, [], []),
        )

        cancellables = list(cancellables_list)

        shared_state.node = InitialAction(
            certificates=certificates[1:],
            n_requested=len(self.children),
            pending=tuple(),
        )
        shared_state.cancellables = tuple(cancellables)

        cancellable = CompositeCancellable(
            cancellables=cancellables,
            certificates=certificates[1:],
        )

        return n_state, ObserveResult(
            cancellable=cancellable, certificate=certificates[0]
        )
