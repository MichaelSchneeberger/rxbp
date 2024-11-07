from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from itertools import accumulate
from threading import RLock
from typing import override

from dataclassabc import dataclassabc
from donotation import do

import continuationmonad
from continuationmonad.typing import (
    Cancellable,
    ContinuationCertificate,
    DeferredSubscription,
)

from rxbp.state import State
from rxbp.flowabletree.data.observer import Observer
from rxbp.flowabletree.nodes import MultiChildrenFlowableNode, FlowableNode
from rxbp.flowabletree.data.observeresult import ObserveResult

type UpstreamID = int


@dataclass(frozen=True)
class OnNextPreState[U]:
    id: UpstreamID
    value: U
    subscription: DeferredSubscription | None
    n_completed: int


@dataclass(frozen=True)
class MergeState:
    pass


@dataclass(frozen=True)
class AwaitNextState(MergeState):
    n_completed: int
    certificates: tuple[ContinuationCertificate, ...]


@dataclass(frozen=True)
class WaitState(MergeState):
    n_completed: int
    certificate: ContinuationCertificate
    certificates: tuple[ContinuationCertificate, ...]


@dataclassabc(frozen=True)
class OnNextState[U](MergeState):
    n_completed: int
    certificates: tuple[ContinuationCertificate, ...]
    id: UpstreamID
    value: U
    subscription: DeferredSubscription | None
    acc_states: list[OnNextPreState]


@dataclassabc(frozen=True)
class OnNextAndCompleteState[U](MergeState):
    certificates: tuple[ContinuationCertificate, ...]
    value: U


@dataclassabc(frozen=True)
class AwaitAckState(MergeState):
    n_completed: int
    certificates: tuple[ContinuationCertificate, ...]
    id: UpstreamID
    acc_states: list[OnNextPreState]
    certificate: ContinuationCertificate


class CompleteState(MergeState):
    pass


@dataclassabc(frozen=True)
class ErrorState(MergeState):
    exception: Exception
    certificates: tuple[ContinuationCertificate, ...]
    awaiting_ids: tuple[UpstreamID, ...]


@dataclassabc(frozen=True)
class TerminatedState(MergeState):
    certificate: ContinuationCertificate
    certificates: tuple[ContinuationCertificate, ...]
    awaiting_ids: tuple[UpstreamID, ...]


@dataclassabc(frozen=True)
class CancelledState(MergeState):
    certificates: dict[UpstreamID, ContinuationCertificate]


# States
########


class MergeAction(ABC):
    @abstractmethod
    def get_state(self) -> MergeState: ...


@dataclass(frozen=True)
class InitAction(MergeAction):
    n_completed: int
    certificates: tuple[ContinuationCertificate, ...]

    @override
    def get_state(self):
        return AwaitNextState(
            n_completed=self.n_completed,
            # certificate=self.certificates[0],
            certificates=self.certificates,
        )


@dataclassabc
class OnNextAction[U](MergeAction):
    child: MergeAction
    id: UpstreamID
    value: U
    subscription: DeferredSubscription

    def get_state(self):
        match child_state := self.child.get_state():
            case (
                OnNextState(
                    id=id,
                    acc_states=acc_states,
                    n_completed=n_completed,
                    certificates=certificates,
                )
                | AwaitAckState(
                    id=id,
                    acc_states=acc_states,
                    n_completed=n_completed,
                    certificates=certificates,
                )
            ):
                pre_state = OnNextPreState(
                    id=self.id,
                    value=self.value,
                    subscription=self.subscription,
                    n_completed=n_completed,
                )
                acc_states.append(pre_state)

                return AwaitAckState(
                    id=id,
                    n_completed=n_completed,
                    acc_states=acc_states,
                    certificate=certificates[0],
                    certificates=certificates[1:],
                )

            case (
                ErrorState(certificates=certificates, awaiting_ids=awaiting_ids)
                | TerminatedState(certificates=certificates, awaiting_ids=awaiting_ids)
            ):
                return TerminatedState(
                    certificate=certificates[0],
                    certificates=certificates[1:],
                    awaiting_ids=tuple(filter(lambda id: id != self.id, awaiting_ids)),
                )

            case CancelledState():
                return child_state

            case (
                WaitState(
                    n_completed=n_completed,
                    certificates=certificates,
                )
                | AwaitNextState(
                    n_completed=n_completed,
                    certificates=certificates,
                )
            ):
                return OnNextState(
                    id=self.id,
                    value=self.value,
                    subscription=self.subscription,
                    acc_states=[],
                    n_completed=n_completed,
                    certificates=certificates,
                )

            case _:
                raise Exception(f"Unexpected state {child_state}.")


@dataclassabc
class OnNextAndCompleteAction[U](MergeAction):
    child: MergeAction
    id: UpstreamID
    value: U
    n_children: int

    def get_state(self):
        match child_state := self.child.get_state():
            case (
                OnNextState(
                    id=id,
                    acc_states=acc_states,
                    n_completed=n_completed,
                    certificates=certificates,
                )
                | AwaitAckState(
                    id=id,
                    acc_states=acc_states,
                    n_completed=n_completed,
                    certificates=certificates,
                )
            ):
                pre_state = OnNextPreState(
                    id=self.id,
                    value=self.value,
                    subscription=None,
                    n_completed=n_completed + 1,
                )
                acc_states.append(pre_state)

                return AwaitAckState(
                    id=id,
                    n_completed=n_completed,
                    acc_states=acc_states,
                    certificate=certificates[0],
                    certificates=certificates[1:],
                )

            case (
                ErrorState(certificates=certificates, awaiting_ids=awaiting_ids)
                | TerminatedState(certificates=certificates, awaiting_ids=awaiting_ids)
            ):
                return TerminatedState(
                    certificate=certificates[0],
                    certificates=certificates[1:],
                    awaiting_ids=tuple(filter(lambda id: id != self.id, awaiting_ids)),
                )

            case CancelledState():
                return child_state

            case (
                WaitState(n_completed=n_completed, certificates=certificates)
                | AwaitNextState(n_completed=n_completed, certificates=certificates)
            ):
                if self.n_children == n_completed + 1:
                    return OnNextAndCompleteState(
                        value=self.value,
                        certificates=certificates,
                    )

                else:
                    return OnNextState(
                        id=self.id,
                        value=self.value,
                        subscription=None,
                        acc_states=[],
                        n_completed=n_completed + 1,
                        certificates=certificates,
                    )
            case _:
                raise Exception(f"Unexpected state {child_state}.")


@dataclassabc(frozen=False)
class AcknowledgeAction(MergeAction):
    id: UpstreamID
    child: MergeAction
    certificate: ContinuationCertificate
    n_children: int

    def get_state(self):
        match child_state := self.child.get_state():
            case (
                OnNextState(
                    id=id,
                    acc_states=acc_states,
                    n_completed=n_completed,
                    certificates=certificates,
                )
                | AwaitAckState(
                    id=id,
                    acc_states=acc_states,
                    n_completed=n_completed,
                    certificates=certificates,
                )
            ):
                assert self.id == id

                match acc_states:
                    case [
                        OnNextPreState(
                            id=id,
                            value=value,
                            subscription=subscription,
                            n_completed=n_completed,
                        ),
                        *others,
                    ]:
                        return OnNextState(
                            id=id,
                            value=value,
                            subscription=subscription,
                            acc_states=others,
                            n_completed=n_completed,
                            certificates=(self.certificate,) + certificates,
                        )

                    case _:
                        return WaitState(
                            n_completed=n_completed,
                            certificate=self.certificate,
                            certificates=certificates,
                            # certificates=(self.certificate,) + certificates,
                        )
            case _:
                raise Exception(f"Unexpected state {child_state}.")


@dataclassabc
class OnCompletedAction(MergeAction):
    child: MergeAction
    id: UpstreamID
    n_children: int

    def get_state(self):
        match child_state := self.child.get_state():
            case (
                WaitState(
                    n_completed=n_completed,
                    certificates=certificates,
                )
                | AwaitNextState(
                    n_completed=n_completed,
                    certificates=certificates,
                )
            ):
                if self.n_children == n_completed + 1:
                    return CompleteState()

                else:
                    return WaitState(
                        n_completed=n_completed + 1,
                        certificate=certificates[0],
                        certificates=certificates[1:],
                    )

            case (
                OnNextState(
                    n_completed=n_completed,
                    acc_states=acc_states,
                    certificates=certificates,
                )
                | AwaitAckState(
                    n_completed=n_completed,
                    acc_states=acc_states,
                    certificates=certificates,
                )
            ):
                return AwaitAckState(
                    id=self.id,
                    acc_states=acc_states,
                    n_completed=n_completed + 1,
                    certificate=certificates[0],
                    certificates=certificates[1:],
                )

            case (
                ErrorState(certificates=certificates, awaiting_ids=awaiting_ids)
                | TerminatedState(certificates=certificates, awaiting_ids=awaiting_ids)
            ):
                return TerminatedState(
                    certificate=certificates[0],
                    certificates=certificates[1:],
                    awaiting_ids=awaiting_ids,
                )

            case CancelledState():
                return child_state

            case _:
                raise Exception(f"Unexpected state {child_state}.")


@dataclassabc(frozen=False)
class OnErrorAction(MergeAction):
    child: MergeAction
    id: UpstreamID
    n_children: int
    exception: Exception

    def get_state(self):
        match child_state := self.child.get_state():
            case (
                OnNextState(acc_states=acc_states, certificates=certificates)
                | AwaitAckState(acc_states=acc_states, certificates=certificates)
            ):
                received_ids = tuple(state.id for state in acc_states)
                awaiting_ids = tuple(
                    id for id in range(self.n_children) if id not in received_ids
                )
                return ErrorState(
                    exception=self.exception,
                    certificates=certificates,
                    awaiting_ids=awaiting_ids,
                )

            case (
                WaitState(certificates=certificates)
                | AwaitNextState(certificates=certificates)
            ):
                awaiting_ids = tuple(range(self.n_children))

                return ErrorState(
                    exception=self.exception,
                    certificates=certificates,
                    awaiting_ids=awaiting_ids,
                )

            case ErrorState(certificates=certificates, awaiting_ids=awaiting_ids):
                return TerminatedState(
                    certificate=certificates[0],
                    certificates=certificates[1:],
                    awaiting_ids=tuple(filter(lambda id: id != self.id, awaiting_ids)),
                )

            case CancelledState():
                return child_state

            case _:
                raise Exception(f"Unexpected state {child_state}.")


@dataclassabc(frozen=False)
class CancelAction(MergeAction):
    child: MergeAction
    n_children: int
    certificate: ContinuationCertificate
    cancellables: dict[UpstreamID, Cancellable]

    def get_state(self):
        match child_state := self.child.get_state():
            case (
                OnNextState(acc_states=acc_states, certificates=certificates)
                | AwaitAckState(acc_states=acc_states, certificates=certificates)
            ):
                received_ids = tuple(state.id for state in acc_states)
                awaiting_ids = tuple(
                    id for id in range(self.n_children) if id not in received_ids
                )
                certificates = certificates + (self.certificate,)

                return CancelledState(
                    certificates=dict(zip(awaiting_ids, certificates)),
                )

            case (
                ErrorState(certificates=certificates, awaiting_ids=awaiting_ids)
                | TerminatedState(certificates=certificates, awaiting_ids=awaiting_ids)
            ):
                certificates = certificates + (self.certificate,)

                return CancelledState(
                    certificates=dict(zip(awaiting_ids, certificates)),
                )

            case CompleteState() | OnNextAndCompleteState():
                return CancelledState(certificates={})

            case _:
                raise Exception(f"Unexpected state {child_state}.")


@dataclassabc(frozen=False)
class MergeSharedMemory(Cancellable):
    downstream: Observer
    action: MergeAction
    n_children: int
    certificates: list[ContinuationCertificate]
    cancellables: dict[UpstreamID, Cancellable]
    lock: RLock

    def cancel(self, certificate: ContinuationCertificate):
        action = CancelAction(
            child=None,  # type: ignore
        )

        with self.lock:
            action.child = self.action
            self.action = action


        state = action.get_state()

        for id, certificate in state.certificates.items():
            self.cancellables[id].cancel(certificate)

@dataclass
class MergeObserver[V](Observer[V]):
    shared: MergeSharedMemory
    id: UpstreamID

    @do()
    def _on_next(self, action: MergeAction):
        match state := action.get_state():
            case WaitState(certificate=certificate):
                return continuationmonad.from_(certificate)

            case AwaitAckState(certificate=certificate):
                return continuationmonad.from_(certificate)

            case OnNextAndCompleteState(value=value):
                return self.shared.downstream.on_next_and_complete(value)

            case OnNextState(value=value, subscription=subscription):
                _ = yield from self.shared.downstream.on_next(value)

                if subscription:
                    certificate, *_ = yield from continuationmonad.from_(None).connect(
                        subscriptions=(subscription,)
                    )

                    action = AcknowledgeAction(
                        child=None,  # type: ignore
                        id=self.id,
                        certificate=certificate,
                        n_children=self.shared.n_children,
                    )

                    with self.shared.lock:
                        action.child = self.shared.action
                        self.shared.action = action

                    return self._on_next(action)

                else:
                    with self.shared.lock:
                        certificate = self.shared.certificates.pop()

                return continuationmonad.from_(certificate)

            case _:
                raise Exception(f"Unexpected state {state}.")

    @do()
    def on_next(self, value: V):
        # wait for upstream subscription before continuing to simplify concurrency
        def on_next_ackowledgment(subscription: DeferredSubscription):
            action = OnNextAction(
                child=None,  # type: ignore
                id=self.id,
                value=value,
                subscription=subscription,
            )

            with self.shared.lock:
                action.child = self.shared.action
                self.shared.action = action

            return self._on_next(action)

        return continuationmonad.defer(on_next_ackowledgment)

    @do()
    def on_next_and_complete(self, value: V):
        print(value)
        action = OnNextAndCompleteAction(
            child=None,  # type: ignore
            id=self.id,
            value=value,
        )

        with self.shared.lock:
            action.child = self.shared.action
            self.shared.action = action

        return self._on_next(action)

    def on_completed(self):
        action = OnCompletedAction(
            child=None,  # type: ignore
            n_children=self.shared.n_children,
            id=self.id,
        )

        with self.shared.lock:
            action.child = self.shared.action
            self.shared.action = action

        match state := action.get_state():
            case CompleteState():
                return self.shared.downstream.on_completed()

            case (
                WaitState(certificate=certificate)
                | AwaitAckState(certificate=certificate)
            ):
                return continuationmonad.from_(certificate)
            
            case TerminatedState(certificate=certificate):
                return continuationmonad.from_(certificate)
            
            case CancelledState(certificates=certificates):
                return continuationmonad.from_(certificates[self.id])
            
            case _:
                raise Exception(f"Unexpected state {state}.")

    def on_error(
        self, exception: Exception
    ):  # -> ContinuationMonad[Continuation | None]:
        action = OnErrorAction(
            child=None,  # type: ignore
            id=self.id,
            n_children=self.shared.n_children,
            exception=exception,
        )

        with self.shared.lock:
            action.child = self.shared.action
            self.shared.action = action

        match state := action.get_state():
            case ErrorState(exception=exception):
                return self.shared.downstream.on_error(exception)

            case TerminatedState(certificate=certificate):
                return continuationmonad.from_(certificate)
            
            case CancelledState(certificates=certificates):
                return continuationmonad.from_(certificates[self.id])

            case _:
                raise Exception(f"Unexpected state {state}.")


class Merge[V](MultiChildrenFlowableNode[V]):
    @do()
    def unsafe_subscribe(
        self, state: State, observer: Observer[V]
    ) -> tuple[State, ObserveResult]:
        shared_state = MergeSharedMemory(
            downstream=observer,
            action=None,  # type: ignore
            n_children=len(self.children),
            certificates=None,  # type: ignore
            cancellables=None,  # type: ignore
            lock=state.lock,
        )

        def acc_continuations(
            acc: tuple[
                State,
                list[ContinuationCertificate],
                list[tuple[UpstreamID, Cancellable]],
            ],
            value: tuple[int, FlowableNode],
        ):
            state, certificates, cancellables = acc
            id, child = value

            observer = MergeObserver(
                shared=shared_state,
                id=id,
            )

            n_state, n_result = child.unsafe_subscribe(state, observer)

            if n_result.certificate:
                certificates.append(n_result.certificate)

            cancellables.append((id, n_result.cancellable))

            return n_state, certificates, cancellables

        *_, (n_state, (first_certificate, *other_certificates), cancellable_pairs) = (
            accumulate(
                func=acc_continuations,
                iterable=enumerate(self.children),
                initial=(state, [], []),
            )
        )

        shared_state.action = InitAction(
            n_completed=0,
            certificates=tuple(other_certificates),
        )
        shared_state.cancellables = dict(cancellable_pairs)

        return n_state, ObserveResult(
            cancellable=shared_state, certificate=first_certificate
        )


@dataclassabc(frozen=True)
class MergeImpl[V](Merge[V]):
    children: tuple[FlowableNode, ...]


def init_merge[V](children: tuple[FlowableNode[V], ...]):
    return MergeImpl[V](children=children)
