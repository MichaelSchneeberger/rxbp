from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import override

from dataclassabc import dataclassabc

from continuationmonad.typing import (
    ContinuationCertificate,
    DeferredObserver,
)

from rxbp.flowabletree.operations.merge.states import (
    AwaitAckBaseState,
    AwaitNextBaseState,
    CancelState,
    CancelledBaseState,
    CompletedBaseState,
    ErrorState,
    HasCancelledState,
    MergeState,
    OnNextNoAckState,
    ErrorBaseState,
    UpstreamID,
    OnNextState,
    AwaitAckState,
    AwaitNextState,
    OnNextPreState,
    OnNextAndCompleteState,
    CompleteState,
    HasTerminatedState,
)


class MergeStateTransition(ABC):
    @abstractmethod
    def get_state(self) -> MergeState: ...


@dataclass(frozen=True)
class ToStateTransition(MergeStateTransition):
    """ Transitions to predefined state """

    state: MergeState

    def get_state(self):
        return self.state


@dataclass(frozen=True)
class InitAction(MergeStateTransition):  # is this needed?
    n_completed: int
    certificates: tuple[ContinuationCertificate, ...]

    @override
    def get_state(self):
        return AwaitNextBaseState(
            n_completed=self.n_completed,
            certificates=self.certificates,
        )


@dataclass
class InactiveTransitionsMixin():
    id: int

    def _get_state(self, state: MergeState):
        match state:
            case ErrorBaseState(
                certificates=certificates,
                awaiting_ids=awaiting_ids,
            ):
                return HasTerminatedState(
                    certificate=certificates[0],
                    certificates=certificates[1:],
                    awaiting_ids=tuple(id for id in awaiting_ids if id != self.id),
                )
            
            case CancelledBaseState(certificates=certificates):
                return HasCancelledState(
                    certificate=certificates[self.id],
                    certificates={id: c for id, c in certificates.items() if id != self.id}
                )

            case _:
                raise Exception(f"Unexpected state {state}.")


@dataclassabc
class OnNextTransition[U](InactiveTransitionsMixin, MergeStateTransition):
    child: MergeStateTransition
    id: UpstreamID
    value: U
    observer: DeferredObserver

    def get_state(self):
        match state := self.child.get_state():
            case AwaitAckBaseState(
                # id=id,
                acc_states=acc_states,
                n_completed=n_completed,
                certificates=certificates,
            ):
                pre_state = OnNextPreState(
                    id=self.id,
                    value=self.value,
                    observer=self.observer,
                    n_completed=n_completed,
                )
                n_acc_states = acc_states + [pre_state]

                return AwaitAckState(
                    # id=id,
                    n_completed=n_completed,
                    acc_states=n_acc_states,
                    certificate=certificates[0],
                    certificates=certificates[1:],
                )

            case AwaitNextBaseState(n_completed=n_completed, certificates=certificates):
                return OnNextState(
                    # id=self.id,
                    value=self.value,
                    observer=self.observer,
                    acc_states=[],
                    n_completed=n_completed,
                    certificates=certificates,
                )

            case _:
                return self._get_state(state)

            # case ErrorBaseState(certificates=certificates, awaiting_ids=awaiting_ids):
            #     return HasTerminatedState(
            #         certificate=certificates[0],
            #         certificates=certificates[1:],
            #         awaiting_ids=tuple(filter(lambda id: id != self.id, awaiting_ids)),
            #     )

            # case CancelState():
            #     return child_state

            # case _:
            #     raise Exception(f"Unexpected state {child_state}.")


@dataclassabc
class OnNextAndCompleteTransition[U](InactiveTransitionsMixin, MergeStateTransition):
    child: MergeStateTransition
    id: UpstreamID
    value: U
    n_children: int

    def get_state(self):
        match child_state := self.child.get_state():
            case AwaitAckBaseState(
                acc_states=acc_states,
                n_completed=n_completed,
                certificates=certificates,
            ):
                pre_state = OnNextPreState(
                    id=self.id,
                    value=self.value,
                    observer=None,
                    n_completed=n_completed + 1,
                )
                n_acc_states = acc_states + [pre_state]

                return AwaitAckState(
                    # id=id,
                    n_completed=n_completed,
                    acc_states=n_acc_states,
                    certificate=certificates[0],
                    certificates=certificates[1:],
                )

            case AwaitNextBaseState(n_completed=n_completed, certificates=certificates):
                if self.n_children == n_completed + 1:
                    return OnNextAndCompleteState(
                        value=self.value,
                    )

                else:
                    return OnNextNoAckState(
                        value=self.value,
                        certificate=certificates[0],
                        acc_states=[],
                        n_completed=n_completed + 1,
                        certificates=certificates[1:],
                    )
                
            case ErrorBaseState(certificates=certificates, awaiting_ids=awaiting_ids):
                return HasTerminatedState(
                    certificate=certificates[0],
                    certificates=certificates[1:],
                    awaiting_ids=tuple(filter(lambda id: id != self.id, awaiting_ids)),
                )

            case CancelState():
                return child_state

            case _:
                raise Exception(f"Unexpected state {child_state}.")


@dataclassabc
class RequestTransition(MergeStateTransition):
    """Downstream request received"""

    id: UpstreamID
    child: MergeStateTransition
    certificate: ContinuationCertificate
    n_children: int

    def get_state(self):
        match previous_state := self.child.get_state():
            case AwaitAckBaseState(
                acc_states=acc_states,
                n_completed=n_completed,
                certificates=certificates,
            ):
                match acc_states:
                    case [
                        OnNextPreState(
                            # id=id,
                            value=value,
                            observer=observer,
                            n_completed=n_completed,
                        ),
                        *others,
                    ]:
                        if observer is None:
                            return OnNextNoAckState(
                                certificate=self.certificate,
                                # id=id,
                                value=value,
                                acc_states=others,
                                n_completed=n_completed,
                                certificates=certificates,
                            )
                        else:
                            return OnNextState(
                                # id=id,
                                value=value,
                                observer=observer,
                                acc_states=others,
                                n_completed=n_completed,
                                certificates=(self.certificate,) + certificates,
                            )

                    case _:  # previous_state should be OnNextState
                        return AwaitNextState(
                            n_completed=n_completed,
                            certificate=self.certificate,  # all other upstream flowables are busy
                            certificates=certificates,
                        )
            
            case CancelState(certificates=certificates):
                return CancelState(certificates=certificates | {self.id: self.certificate})

            case _:
                raise Exception(f"Unexpected state {previous_state}.")


@dataclassabc
class OnCompletedTransition(InactiveTransitionsMixin, MergeStateTransition):
    child: MergeStateTransition
    id: UpstreamID
    n_children: int

    def get_state(self):
        match child_state := self.child.get_state():
            case AwaitAckBaseState(
                acc_states=acc_states,
                n_completed=n_completed,
                certificates=certificates,
            ):
                return AwaitAckState(
                    acc_states=acc_states,
                    n_completed=n_completed + 1,
                    certificate=certificates[0],
                    certificates=certificates[1:],
                )
            
            case AwaitNextBaseState(n_completed=n_completed, certificates=certificates):
                if self.n_children == n_completed + 1:
                    return CompleteState()

                else:
                    return AwaitNextState(
                        n_completed=n_completed + 1,
                        certificate=certificates[0],
                        certificates=certificates[1:],
                    )

            case ErrorBaseState(certificates=certificates, awaiting_ids=awaiting_ids):
                return HasTerminatedState(
                    certificate=certificates[0],
                    certificates=certificates[1:],
                    awaiting_ids=awaiting_ids,
                )

            case CancelState():
                return child_state

            case _:
                raise Exception(f"Unexpected state {child_state}.")


@dataclassabc(frozen=False)
class OnErrorTransition(InactiveTransitionsMixin, MergeStateTransition):
    child: MergeStateTransition
    id: UpstreamID
    n_children: int
    exception: Exception

    def get_state(self):
        match child_state := self.child.get_state():
            case AwaitAckBaseState(acc_states=acc_states, certificates=certificates):
                received_ids = tuple(state.id for state in acc_states)
                awaiting_ids = tuple(
                    id for id in range(self.n_children) if id not in received_ids
                )
                return ErrorState(
                    exception=self.exception,
                    certificates=certificates,
                    awaiting_ids=awaiting_ids,
                )

            case AwaitNextBaseState(certificates=certificates):
                awaiting_ids = tuple(range(self.n_children))

                return ErrorState(
                    exception=self.exception,
                    certificates=certificates,
                    awaiting_ids=awaiting_ids,
                )

            case ErrorBaseState(certificates=certificates, awaiting_ids=awaiting_ids):
                # mark as terminated to not probagate more than one error
                return HasTerminatedState(
                    certificate=certificates[0],
                    certificates=certificates[1:],
                    awaiting_ids=tuple(filter(lambda id: id != self.id, awaiting_ids)),
                )

            case CancelState():
                return child_state

            case _:
                raise Exception(f"Unexpected state {child_state}.")


# @dataclass
# class AwaitingIDsMixin:
#     n_children: int

#     def _get_awaiting_ids(
#         self,
#         values: dict[int, None],
#         id: int | None = None,
#     ):
#         received_ids = tuple(values.keys())

#         if id is not None:
#             received_ids += (id,)

#         awaiting_ids = tuple(
#             id for id in range(self.n_children) if id not in received_ids
#         )
#         return awaiting_ids


@dataclassabc(frozen=False)
class CancelTransition(InactiveTransitionsMixin, MergeStateTransition):
    child: MergeStateTransition
    n_children: int
    certificate: ContinuationCertificate

    def get_state(self):
        match child_state := self.child.get_state():
            case AwaitAckBaseState(acc_states=acc_states, certificates=certificates):
                received_ids = tuple(state.id for state in acc_states)
                awaiting_ids = tuple(
                    id for id in range(self.n_children) if id not in received_ids
                )
                certificates = certificates + (self.certificate,)

                return CancelState(
                    certificates=dict(zip(awaiting_ids, certificates)),
                )

            case ErrorBaseState(certificates=certificates, awaiting_ids=awaiting_ids):
                certificates = certificates + (self.certificate,)

                return CancelState(
                    certificates=dict(zip(awaiting_ids, certificates)),
                )

            case CompletedBaseState():
                return CancelState(certificates={})

            case _:
                raise Exception(f"Unexpected state {child_state}.")


@dataclassabc(frozen=False)
class SubscribeTransition(MergeStateTransition):
    id: UpstreamID
    child: MergeStateTransition
    certificate: ContinuationCertificate

    def get_state(self):
        match state := self.child.get_state():
            case AwaitAckBaseState():
                return AwaitAckBaseState(
                    n_completed=state.n_completed,
                    certificates=state.certificates + (self.certificate,),
                    acc_states=state.acc_states,
                )

            case AwaitNextBaseState():
                return AwaitNextBaseState(
                    n_completed=state.n_completed,
                    certificates=state.certificates + (self.certificate,),
                )

            case ErrorBaseState(certificates=certificates, awaiting_ids=awaiting_ids):
                certificates = certificates + (self.certificate,)

                return ErrorBaseState(
                    certificates=certificates,
                    awaiting_ids=awaiting_ids + (self.id,)
                )

            case CompletedBaseState():
                return CompletedBaseState()
            
            case CancelState(certificates=certificates):
                return CancelState(certificates | {self.id: self.certificate})

            case _:
                raise Exception(f"Unexpected state {state}.")
