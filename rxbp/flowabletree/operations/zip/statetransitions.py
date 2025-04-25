from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass

from dataclassabc import dataclassabc

from continuationmonad.typing import (
    ContinuationCertificate,
    DeferredObserver,
)

from rxbp.flowabletree.operations.zip.states import (
    AwaitOnNextState,
    CancelState,
    CancelledBaseState,
    CancelledAwaitRequestState,
    TerminatedBaseState,
    OnNextAndCompleteState,
    HasTerminatedState,
    OnCompletedState,
    OnErrorState,
    OnNextState,
    AwaitFurtherState,
    ZipState,
    RequestState,
)

"""
State Machine:

States: 
- Request: Request item from non-backpressured upstream Flowables.
- AwaitFurther: Await further on_next calls from upstream.
- OnNext: Either call downstream on_next or on_next_and_complete method.
- OnCompleted: Call downstream on_completed method.
- OnError: Call downstream on_error method.
- Terminated (Zip is either completed or errors): Suppress any continuations from upstream.
- CancelAwaitRequest: Await request to assign certificates to each active upstream.
- Cancel: Cancel upstream Flowables.

Transitions:
- on_next: Request, AwaitFurther -> OnNext          if all values have been received
                                 -> AwaitFurther
           _                     -> StopContinuation
- request: OnNext -> Request
           Cancel -> Cancel
- on_next_and_complete: Request, AwaitFurther -> OnNext     if all values have been received
                                              -> AwaitFurther
                        _                     -> StopContinuation
- on_completed: Request, AwaitFurther                          -> OnCompleted
                OnError, OnCompleted, Cancel, StopContinuation -> StopContinuation
- on_error: Request, AwaitFurther                          -> OnError
            OnError, OnCompleted, Cancel, StopContinuation -> StopContinuation
- cancel: Request, AwaitFurther, OnNext, OnCompleted, OnError, StopContinuation -> Cancel
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
class AwaitingIDsMixin:
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
            case TerminatedBaseState(
                certificates=certificates,
                # awaiting_ids=awaiting_ids,
            ):
            #     return HasTerminatedState(
            #         certificate=certificates[0],
            #         certificates=certificates[1:],
            #         awaiting_ids=tuple(id for id in awaiting_ids if id != self.id),
            #     )
            
            # case CancelledBaseState(certificates=certificates):
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
            case AwaitOnNextState(
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
                            # certificates=tuple(),
                            # awaiting_ids=tuple(),
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
class RequestTransition(AwaitingIDsMixin, ZipStateTransition):
    """Downstream requests new item"""

    child: ZipStateTransition
    values: dict[int, None]
    observers: dict[int, DeferredObserver]
    certificates: tuple[ContinuationCertificate, ...]

    def get_state(self):
        match state := self.child.get_state():
            case OnNextState():
                return RequestState(
                    values=self.values,
                    observers=self.observers,
                    certificates=self.certificates,
                    is_completed=False,
                )

            case CancelledAwaitRequestState(certificate=certificate):
                # awaiting_ids = self._get_awaiting_ids(self.values)
                # certificates = self.certificates + (certificate,)
                return CancelState(
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
            case AwaitOnNextState(
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
                        # awaiting_ids=tuple(),
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
class OnCompletedTransition(InactiveTransitionsMixin, AwaitingIDsMixin, ZipStateTransition):
    child: ZipStateTransition

    def get_state(self):
        match state := self.child.get_state():
            case AwaitOnNextState(
                values=values,
                certificates=certificates,
            ):
                return OnCompletedState(
                    certificates=self._assign_certificates(
                        values=values, 
                        certificates=certificates,
                    ),
                    # certificates=certificates,
                    # awaiting_ids=self._get_awaiting_ids(values, id=self.id),
                )

            case _:
                return self._get_state(state)


@dataclass
class OnErrorTransition(InactiveTransitionsMixin, AwaitingIDsMixin, ZipStateTransition):
    child: ZipStateTransition
    exception: Exception

    def get_state(self):
        match state := self.child.get_state():
            case AwaitFurtherState(
                certificates=certificates,
                values=values,
            ):
                return OnErrorState(
                    exception=self.exception,
                    certificates=self._assign_certificates(
                        values=values, 
                        certificates=certificates,
                    ),
                    # certificates=certificates,
                    # awaiting_ids=self._get_awaiting_ids(values, id=self.id),
                )

            # case TerminatedBaseState(
            #     certificates=certificates,
            #     awaiting_ids=awaiting_ids,
            # ):
            #     return HasTerminatedState(
            #         certificate=certificates[0],
            #         certificates=certificates[1:],
            #         awaiting_ids=tuple(id for id in awaiting_ids if id != self.id),
            #     )
            
            # case CancelledBaseState(certificates=certificates):
            #     return HasCancelledState(
            #         certificate=certificates[self.id],
            #         certificates={id: c for id, c in certificates.items() if id != self.id}
            #     )

            case _:
                return self._get_state(state)


@dataclass
class CancelTransition(AwaitingIDsMixin, ZipStateTransition):
    child: ZipStateTransition
    certificate: ContinuationCertificate

    def get_state(self):
        match child_state := self.child.get_state():
            case AwaitOnNextState(
                values=values,
                certificates=certificates,
            ):
                certificates=self._assign_certificates(
                    values=values, 
                    certificates=certificates + (self.certificate,),
                )
                return CancelState(
                    certificates=certificates,
                )

            case OnNextState():
                return CancelledAwaitRequestState(
                    # certificate=self.certificate,
                    certificates={},
                )

            case TerminatedBaseState(
                # awaiting_ids=awaiting_ids,
                certificates=certificates,
            ):
                return CancelState(
                    certificates=certificates,
                )

            case _:
                raise Exception(f"Unexpected state {child_state}.")

        # certificates = certificates + (self.certificate,)
        # return CancelledBaseState(
        #     certificates=dict(zip(awaiting_ids, certificates)),
        # )
