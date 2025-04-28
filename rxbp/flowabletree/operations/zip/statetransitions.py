from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass

from dataclassabc import dataclassabc

from continuationmonad.typing import (
    ContinuationCertificate,
    DeferredObserver,
)

from rxbp.flowabletree.operations.zip.states import (
    AwaitUpstreamStateMixin,
    CancelledAwaitRequestState,
    CancelledState,
    TerminatedStateMixin,
    OnNextAndCompleteState,
    HasTerminatedState,
    OnCompletedState,
    OnErrorState,
    OnNextState,
    AwaitFurtherState,
    ZipState,
    AwaitOnNextState,
)

"""
State Machine
-------------

State groups (-), states (>):
- Active - AwaitUpstream:
  > AwaitOnNext
  > AwaitFurther
- Active - AwaitDownstream:
  > OnNext
- Terminated:
  > OnNextAndComplete
  > OnCompleted
  > OnError
  > Cancelled
  > CancelledAwaitRequest
  > HasTerminated


  Transitions:
  - on_next:
        AwaitUpstream           -> OnNext
                                -> OnNextAndComplete        if one upstream completed
                                -> AwaitFurther             if not all items are received
        Terminated              -> HasTerminated
- request:
        AwaitDownstream          -> AwaitOnNext
        CancelledAwaitRequest   -> Cancelled
        Terminated              -> HasTerminated
- on_next_and_complete: 
        AwaitUpstream           -> AwaitFuther
                                -> OnNextAndComplete        if all upstream completed
        Terminated              -> HasTerminated
- on_completed:
        AwaitUpstream           -> OnCompleted
        Terminated              -> HasTerminated
- on_error:
        AwaitUpstream           -> OnError
        Terminated              -> HasTerminated
- cancel:
        AwaitUpstream           -> Cancelled
        AwaitDownstream         -> CancelledAwaitRequest
        Terminated              -> Cancelled
"""


class ZipStateTransition(ABC):
    @abstractmethod
    def get_state(self) -> ZipState: ...


@dataclassabc(frozen=True)
class ToStateTransition(ZipStateTransition):
    """Transitions to predefined state"""

    state: ZipState

    def get_state(self):
        return self.state


@dataclass
class AssignCertificatesMixin:
    n_children: int

    def _get_awaiting_ids(
        self,
        values: dict[int, None],
        id: int | None = None,
    ):
        received_ids = tuple(values.keys())

        if id is not None:
            received_ids += (id,)

        awaiting_ids = tuple(
            id for id in range(self.n_children) if id not in received_ids
        )
        return awaiting_ids
    
    def _assign_certificates(
        self,
        values: dict[int, None],
        certificates: tuple[ContinuationCertificate, ...],
        id: int | None = None,
    ):
        awaiting_ids = self._get_awaiting_ids(values, id)
        return dict(zip(awaiting_ids, certificates))
    

@dataclass
class InactiveTransitionsMixin():
    id: int

    def _get_state(self, state: ZipState):
        match state:
            case TerminatedStateMixin(
                certificates=certificates,
            ):
                return HasTerminatedState(
                    certificate=certificates[self.id],
                    certificates={id: c for id, c in certificates.items() if id != self.id}
                )

            case _:
                raise Exception(f"Unexpected state {state}.")

@dataclass
class OnNextTransition[U](InactiveTransitionsMixin, ZipStateTransition):
    child: ZipStateTransition
    value: U
    observer: DeferredObserver
    n_children: int

    def get_state(self):
        match state := self.child.get_state():
            case AwaitUpstreamStateMixin(
                certificates=certificates,
                is_completed=is_completed,
            ):
                values = state.values | {self.id: self.value}
                observers = state.observers | {self.id: self.observer}

                if len(values) == self.n_children:
                    assert len(certificates) == 0

                    if is_completed:
                        return OnNextAndCompleteState(
                            values=values,
                            observers=observers,
                            certificates={},
                        )

                    else:
                        return OnNextState(
                            values=values,
                            observers=observers,
                        )

                else:
                    return AwaitFurtherState(
                        values=values,
                        observers=observers,
                        certificate=certificates[0],
                        certificates=certificates[1:],
                        is_completed=is_completed,
                    )

            case _:
                return self._get_state(state)


@dataclass
class RequestTransition(AssignCertificatesMixin, ZipStateTransition):
    """Downstream requests new item"""

    child: ZipStateTransition
    values: dict[int, None]
    observers: dict[int, DeferredObserver]
    certificates: tuple[ContinuationCertificate, ...]

    def get_state(self):
        match state := self.child.get_state():
            case OnNextState():
                return AwaitOnNextState(
                    values=self.values,
                    observers=self.observers,
                    certificates=self.certificates,
                    is_completed=False,
                )

            case CancelledAwaitRequestState(certificate=certificate):
                return CancelledState(
                    certificates=self._assign_certificates(
                        values=self.values, 
                        certificates=self.certificates + (certificate,),
                    ),
                )

            case _:
                raise Exception(f"Unexpected state {state}.")


@dataclass
class OnNextAndCompleteTransition[U](InactiveTransitionsMixin, ZipStateTransition):
    child: ZipStateTransition
    n_children: int
    value: U

    def get_state(self):
        match state := self.child.get_state():
            case AwaitUpstreamStateMixin(
                certificates=certificates,
            ):
                values = state.values | {self.id: self.value}
                observers = state.observers

                if len(values) == self.n_children:
                    assert len(certificates) == 0

                    return OnNextAndCompleteState(
                        values=values,
                        observers=observers,
                        certificates={},
                    )

                else:
                    return AwaitFurtherState(
                        certificate=certificates[0],
                        certificates=certificates[1:],
                        values=values,
                        observers=observers,
                        is_completed=True,
                    )

            case _:
                return self._get_state(state)


@dataclass
class OnCompletedTransition(InactiveTransitionsMixin, AssignCertificatesMixin, ZipStateTransition):
    child: ZipStateTransition

    def get_state(self):
        match state := self.child.get_state():
            case AwaitUpstreamStateMixin(
                values=values,
                certificates=certificates,
            ):
                return OnCompletedState(
                    certificates=self._assign_certificates(
                        values=values, 
                        certificates=certificates,
                    ),
                )

            case _:
                return self._get_state(state)


@dataclass
class OnErrorTransition(InactiveTransitionsMixin, AssignCertificatesMixin, ZipStateTransition):
    child: ZipStateTransition
    exception: Exception

    def get_state(self):
        match state := self.child.get_state():
            case AwaitUpstreamStateMixin(
                certificates=certificates,
                values=values,
            ):
                return OnErrorState(
                    exception=self.exception,
                    certificates=self._assign_certificates(
                        values=values, 
                        certificates=certificates,
                    ),
                )

            case _:
                return self._get_state(state)


@dataclass
class CancelTransition(AssignCertificatesMixin, ZipStateTransition):
    child: ZipStateTransition
    certificate: ContinuationCertificate

    def get_state(self):
        match state := self.child.get_state():
            case AwaitUpstreamStateMixin(
                values=values,
                certificates=certificates,
            ):
                certificates=self._assign_certificates(
                    values=values, 
                    certificates=certificates + (self.certificate,),
                )
                return CancelledState(
                    certificates=certificates,
                )

            case OnNextState():
                return CancelledAwaitRequestState(
                    certificate=self.certificate,
                )

            case TerminatedStateMixin(certificates=certificates):
                return CancelledState(
                    certificates=certificates,
                )

            case _:
                raise Exception(f"Unexpected state {state}.")
