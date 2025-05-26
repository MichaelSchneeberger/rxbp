from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass

from dataclassabc import dataclassabc

from continuationmonad.typing import (
    ContinuationCertificate,
    DeferredHandler,
)

from rxbp.flowabletree.operations.flatmap.states import (
    AwaitDownstreamOuterCompletedState,
    AwaitDownstreamStateMixin,
    AwaitUpstreamStateMixin,
    CancelledStopRequestState,
    SubscribedState,
    CancelledAwaitRequestState,
    CancelledState,
    HasTerminatedState,
    FlatMapState,
    OnCompletedState,
    OnErrorState,
    SubscribedStateOuterCompleted,
    TerminatedStateMixin,
    OnNextState,
    KeepWaitingState,
    AwaitOnNextState,
    BackpressuredOnNextCalls,
    OnNextAndCompleteState,
)


class FlatMapStateTransition(ABC):
    @abstractmethod
    def get_state(self) -> FlatMapState: ...


@dataclass(frozen=True)
class ToStateTransition(FlatMapStateTransition):
    """Transitions to predefined state"""

    state: FlatMapState

    def get_state(self):
        return self.state


@dataclass
class InactiveTransitionsMixin:
    id: int

    def _get_state(self, state: FlatMapState):
        match state:
            case TerminatedStateMixin(
                certificates=certificates,
                outer_certificate=outer_certificate,
            ):
                return HasTerminatedState(
                    certificate=certificates[self.id],
                    certificates={
                        id: c for id, c in certificates.items() if id != self.id
                    },
                    outer_certificate=outer_certificate,
                )

            case _:
                raise Exception(f"Unexpected state {state}.")


@dataclassabc
class OnNextTransition[U](InactiveTransitionsMixin, FlatMapStateTransition):
    child: FlatMapStateTransition
    id: int
    value: U
    observer: DeferredHandler

    def get_state(self):
        match state := self.child.get_state():
            case AwaitUpstreamStateMixin(
                active_ids=active_ids,
                certificates=certificates,
                is_outer_completed=is_outer_completed,
            ):
                return OnNextState(
                    value=self.value,
                    observer=self.observer,
                    on_next_calls=tuple(),
                    active_ids=active_ids,
                    certificates=certificates,
                    is_outer_completed=is_outer_completed,
                )

            case AwaitDownstreamStateMixin(
                on_next_calls=on_next_calls,
                active_ids=active_ids,
                certificates=certificates,
                is_outer_completed=is_outer_completed,
            ):
                # backpressure on_next call

                pre_state = BackpressuredOnNextCalls(
                    id=self.id,
                    value=self.value,
                    observer=self.observer,
                )
                n_on_next_calls = on_next_calls + (pre_state,)

                return KeepWaitingState(
                    active_ids=active_ids,
                    on_next_calls=n_on_next_calls,
                    certificate=certificates[0],
                    certificates=certificates[1:],
                    is_outer_completed=is_outer_completed,
                )

            case _:
                return self._get_state(state)


@dataclassabc
class RequestTransition(FlatMapStateTransition):
    """Downstream request received"""

    id: int
    child: FlatMapStateTransition
    certificate: ContinuationCertificate | None

    def get_state(self):
        match previous_state := self.child.get_state():
            case AwaitDownstreamStateMixin(
                on_next_calls=on_next_calls,
                active_ids=active_ids,
                certificates=certificates,
                is_outer_completed=is_outer_completed,
            ):
                match on_next_calls:
                    case [
                        BackpressuredOnNextCalls(
                            value=value,
                            observer=observer,
                        ),
                        *others,
                    ]:
                        # backpressured upstream exist

                        if observer is None:
                            # upstream called on_next_and_complete

                            # last element in buffer
                            if is_outer_completed and len(active_ids):
                                return OnNextAndCompleteState(
                                    certificates={},
                                    value=value,
                                    outer_certificate=None,
                                )

                            else:
                                return OnNextState(
                                    observer=None,
                                    value=value,
                                    on_next_calls=tuple(others),
                                    active_ids=active_ids,
                                    certificates=certificates + (self.certificate,)
                                    if self.certificate
                                    else certificates,
                                    is_outer_completed=is_outer_completed,
                                )

                        else:
                            return OnNextState(
                                value=value,
                                observer=observer,
                                on_next_calls=tuple(others),
                                active_ids=active_ids,
                                certificates=certificates + (self.certificate,)
                                if self.certificate
                                else certificates,
                                is_outer_completed=is_outer_completed,
                            )

                    case _:
                        # no backpressured upstream exist

                        if self.certificate:
                            return AwaitOnNextState(
                                active_ids=active_ids,
                                certificate=self.certificate,
                                certificates=certificates,
                                is_outer_completed=is_outer_completed,
                            )

                        else:
                            return AwaitOnNextState(
                                active_ids=active_ids,
                                certificate=certificates[0],
                                certificates=certificates[1:],
                                is_outer_completed=is_outer_completed,
                            )

            case CancelledAwaitRequestState(
                certificates=certificates,
                certificate=certificate,
                outer_certificate=outer_certificate,
            ):
                if self.certificate:
                    return CancelledStopRequestState(
                        certificate=certificate,
                        certificates={self.id: self.certificate} | certificates,
                        outer_certificate=outer_certificate,
                    )
                else:
                    return CancelledStopRequestState(
                        certificate=certificate,
                        certificates=certificates,
                        outer_certificate=outer_certificate,
                    )

            case _:
                raise Exception(f"Unexpected state {previous_state}.")


@dataclassabc
class OnNextAndCompleteTransition[U](InactiveTransitionsMixin, FlatMapStateTransition):
    child: FlatMapStateTransition
    id: int
    value: U

    def get_state(self):
        match state := self.child.get_state():
            case AwaitUpstreamStateMixin(
                active_ids=active_ids,
                certificates=certificates,
                is_outer_completed=is_outer_completed,
            ):
                if is_outer_completed and len(active_ids) == 1:
                    return OnNextAndCompleteState(
                        value=self.value,
                        certificates={},
                        outer_certificate=None,
                    )

                else:
                    return OnNextState(
                        value=self.value,
                        observer=None,
                        on_next_calls=tuple(),
                        active_ids=tuple(id for id in active_ids if id != self.id),
                        certificates=certificates,
                        is_outer_completed=is_outer_completed,
                    )

            case AwaitDownstreamStateMixin(
                on_next_calls=on_next_calls,
                active_ids=active_ids,
                certificates=certificates,
                is_outer_completed=is_outer_completed,
            ):
                # backpressure on_next call

                pre_state = BackpressuredOnNextCalls(
                    id=self.id,
                    value=self.value,
                    observer=None,
                )
                n_on_next_calls = on_next_calls + (pre_state,)

                return KeepWaitingState(
                    active_ids=tuple(id for id in active_ids if id != self.id),
                    on_next_calls=n_on_next_calls,
                    certificate=certificates[0],
                    certificates=certificates[1:],
                    is_outer_completed=is_outer_completed,
                )

            case _:
                return self._get_state(state)


@dataclassabc
class OnCompletedTransition(InactiveTransitionsMixin, FlatMapStateTransition):
    child: FlatMapStateTransition
    id: int

    def get_state(self):
        match state := self.child.get_state():
            case AwaitUpstreamStateMixin(
                active_ids=active_ids,
                certificates=certificates,
                is_outer_completed=is_outer_completed,
            ):
                if is_outer_completed and len(active_ids) == 1:
                    return OnCompletedState(
                        certificates={},
                        outer_certificate=None,
                    )

                else:
                    return AwaitOnNextState(
                        active_ids=tuple(id for id in active_ids if id != self.id),
                        certificate=certificates[0],
                        certificates=certificates[1:],
                        is_outer_completed=is_outer_completed,
                    )

            case AwaitDownstreamStateMixin(
                on_next_calls=on_next_calls,
                active_ids=active_ids,
                certificates=certificates,
                is_outer_completed=is_outer_completed,
            ):
                return KeepWaitingState(
                    on_next_calls=on_next_calls,
                    active_ids=tuple(id for id in active_ids if id != self.id),
                    certificate=certificates[0],
                    certificates=certificates[1:],
                    is_outer_completed=is_outer_completed,
                )

            case _:
                return self._get_state(state)


@dataclassabc(frozen=False)
class OnErrorTransition(InactiveTransitionsMixin, FlatMapStateTransition):
    id: int
    child: FlatMapStateTransition
    exception: Exception

    def get_state(self):
        match state := self.child.get_state():
            case AwaitUpstreamStateMixin(
                certificates=certificates,
                is_outer_completed=is_outer_completed,
            ):
                active_ids = tuple(id for id in state.active_ids if id != self.id)

            case AwaitDownstreamStateMixin(
                on_next_calls=on_next_calls, 
                certificates=certificates,
                is_outer_completed=is_outer_completed,
            ):
                received_ids = tuple(call.id for call in on_next_calls) + (self.id,)
                active_ids = tuple(
                    id for id in state.active_ids if id not in received_ids
                )

            case _:
                return self._get_state(state)
            
        if is_outer_completed:
            return OnErrorState(
                exception=self.exception,
                certificates=dict(zip(active_ids, certificates, strict=True)),
                outer_certificate=None,
            )
        
        else:
            return OnErrorState(
                exception=self.exception,
                certificates=dict(zip(active_ids, certificates[1:], strict=True)),
                outer_certificate=certificates[0],
            )


@dataclassabc(frozen=False)
class OnNextOuterTransition(FlatMapStateTransition):
    id: int
    child: FlatMapStateTransition
    certificate: ContinuationCertificate

    def get_state(self):
        match state := self.child.get_state():
            case AwaitUpstreamStateMixin(
                active_ids=active_ids,
                is_outer_completed=is_outer_completed,
                certificates=certificates
            ):
                assert is_outer_completed is False

                return SubscribedState(
                    active_ids=active_ids + (self.id,),
                    certificates=state.certificates + (self.certificate,),
                    is_outer_completed=False,
                )

            case AwaitDownstreamStateMixin(
                active_ids=active_ids,
                is_outer_completed=is_outer_completed,
            ):
                assert is_outer_completed is False

                return AwaitDownstreamStateMixin(
                    active_ids=active_ids + (self.id,),
                    certificates=state.certificates + (self.certificate,),
                    on_next_calls=state.on_next_calls,
                    is_outer_completed=False,
                )

            case TerminatedStateMixin(
                certificates=certificates,
                outer_certificate=outer_certificate,
            ):
                return HasTerminatedState(
                    certificate=outer_certificate,
                    certificates=certificates,
                    outer_certificate=None,
                )

            case _:
                raise Exception(f"Unexpected state {state}.")

@dataclassabc
class OnNextAndCompleteOuterTransition[U](FlatMapStateTransition):
    id: int
    child: FlatMapStateTransition
    certificate: ContinuationCertificate

    def get_state(self):
        match state := self.child.get_state():
            case AwaitUpstreamStateMixin(
                active_ids=active_ids,
                is_outer_completed=is_outer_completed,
            ):
                assert is_outer_completed is False

                return SubscribedStateOuterCompleted(
                    active_ids=active_ids + (self.id,),
                    certificates=state.certificates,
                    is_outer_completed=True,
                    certificate=self.certificate,
                )

            case AwaitDownstreamStateMixin(
                active_ids=active_ids,
                is_outer_completed=is_outer_completed,
            ):
                assert is_outer_completed is False

                return AwaitDownstreamOuterCompletedState(
                    active_ids=active_ids + (self.id,),
                    certificates=state.certificates,
                    on_next_calls=state.on_next_calls,
                    is_outer_completed=True,
                    certificate=self.certificate,
                )

            case TerminatedStateMixin(
                certificates=certificates,
                outer_certificate=outer_certificate,
            ):
                return HasTerminatedState(
                    certificate=outer_certificate,
                    certificates=certificates | {self.id: self.certificate},
                    outer_certificate=None,
                )

            case _:
                raise Exception(f"Unexpected state {state}.")


@dataclassabc
class OnCompletedOuterTransition(FlatMapStateTransition):
    child: FlatMapStateTransition

    def get_state(self):
        match state := self.child.get_state():
            case AwaitUpstreamStateMixin(
                active_ids=active_ids,
                certificates=certificates,
            ):
                if len(active_ids) == 0:
                    return OnCompletedState(
                        certificates={},
                        outer_certificate=None,
                    )

                else:
                    return AwaitOnNextState(
                        active_ids=active_ids,
                        certificate=certificates[0],
                        certificates=certificates[1:],
                        is_outer_completed=True,
                    )

            case AwaitDownstreamStateMixin(
                on_next_calls=on_next_calls,
                active_ids=active_ids,
                certificates=certificates,
            ):
                return KeepWaitingState(
                    on_next_calls=on_next_calls,
                    active_ids=active_ids,
                    certificate=certificates[0],
                    certificates=certificates[1:],
                    is_outer_completed=True,
                )

            case TerminatedStateMixin(
                certificates=certificates,
                outer_certificate=outer_certificate,
            ):
                return HasTerminatedState(
                    certificate=outer_certificate,
                    certificates=certificates,
                    outer_certificate=None,
                )

            case _:
                raise Exception(f"Unexpected state {state}.")


@dataclassabc(frozen=False)
class OnErrorOuterTransition(FlatMapStateTransition):
    child: FlatMapStateTransition
    exception: Exception

    def get_state(self):
        match state := self.child.get_state():
            case AwaitUpstreamStateMixin(
                certificates=certificates,
                active_ids=active_ids,
            ):
                return OnErrorState(
                    exception=self.exception,
                    certificates=dict(zip(active_ids, certificates, strict=True)),
                    outer_certificate=None,
                )

            case AwaitDownstreamStateMixin(
                on_next_calls=on_next_calls, 
                certificates=certificates,
            ):
                received_ids = tuple(call.id for call in on_next_calls)
                active_ids = tuple(
                    id for id in state.active_ids if id not in received_ids
                )

                return OnErrorState(
                    exception=self.exception,
                    certificates=dict(zip(active_ids, certificates, strict=True)),
                    outer_certificate=None,
                )

            case TerminatedStateMixin(
                certificates=certificates,
                outer_certificate=outer_certificate,
            ):
                return HasTerminatedState(
                    certificate=outer_certificate,
                    certificates=certificates,
                    outer_certificate=None,
                )

            case _:
                raise Exception(f"Unexpected state {state}.")



@dataclassabc(frozen=False)
class CancelTransition(FlatMapStateTransition):
    child: FlatMapStateTransition
    certificate: ContinuationCertificate

    def get_state(self):
        match state := self.child.get_state():
            case AwaitUpstreamStateMixin(
                certificates=certificates,
                active_ids=active_ids,
                is_outer_completed=is_outer_completed,
            ):
                certificates = certificates + (self.certificate,)

                if is_outer_completed:
                    return CancelledState(
                        certificates=dict(zip(active_ids, certificates, strict=True)),
                        outer_certificate=None,
                    )
                else:
                    return CancelledState(
                        certificates=dict(zip(active_ids, certificates[1:], strict=True)),
                        outer_certificate=certificates[0],
                    )

            case AwaitDownstreamStateMixin(
                on_next_calls=on_next_calls,
                certificates=certificates,
                is_outer_completed=is_outer_completed,
            ):
                received_ids = tuple(call.id for call in on_next_calls)
                active_ids = tuple(
                    id for id in state.active_ids if id not in received_ids
                )

                if is_outer_completed:
                    return CancelledAwaitRequestState(
                        certificate=self.certificate,
                        certificates=dict(zip(active_ids, certificates, strict=True)),
                        outer_certificate=None,
                    )
                
                else:
                    return CancelledAwaitRequestState(
                        certificate=self.certificate,
                        certificates=dict(zip(active_ids, certificates[1:], strict=True)),
                        outer_certificate=certificates[0],
                    )

            case TerminatedStateMixin(
                certificates=certificates,
                outer_certificate=outer_certificate,
            ):
                return CancelledState(
                    certificates=certificates,
                    outer_certificate=outer_certificate,
                )

            case _:
                raise Exception(f"Unexpected state {state}.")
            
