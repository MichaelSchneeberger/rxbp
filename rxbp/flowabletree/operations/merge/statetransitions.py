from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass

from dataclassabc import dataclassabc

from continuationmonad.typing import (
    ContinuationCertificate,
    DeferredHandler,
)

from rxbp.flowabletree.operations.merge.states import (
    AwaitUpstreamStateMixin,
    AwaitOnNextState,
    AwaitDownstreamStateMixin,
    OnNextState,
    KeepWaitingState,
    TerminatedStateMixin,
    HasTerminatedState,
    MergeState,
    OnCompletedState,
    OnErrorState,
    BackpressuredOnNextCalls,
    OnNextAndCompleteState,
    CancelledStopRequestState,
    CancelledAwaitRequestState,
    CancelledState,
)

"""
State Machine
-------------

State groups (-), states (>):
- Active - AwaitUpstream
  > Init
  > AwaitOnNext
- Active - AwaitDownstream
  > OnNext
  > KeepWaiting
- Terminated
  > OnNextAndComplete
  > OnCompleted
  > OnError
  > Cancelled
  > CancelledAwaitRequest
  > CancelledStopRequest
  > HasTerminated

Transitions:
- on_next:
        AwaitUpstream           -> OnNext
        AwaitDownstream         -> KeepWaiting
        Terminated              -> HasTerminated
- request:
        AwaitDownstream         -> OnNext
                                -> OnNextAndComplete        if all upstream completed
                                -> AwaitOnNext              if no other items were received
        CancelledAwaitRequest   -> CancelledStopRequest
- on_next_and_complete: 
        AwaitUpstream           -> OnNext
        AwaitDownstream         -> OnNextAndComplete        if all upstream completed
        Terminated              -> HasTerminated
- on_completed:
        AwaitUpstream           -> AwaitOnNext
                                -> OnCompleted              if all upstream completed
        AwaitDownstream         -> KeepWaiting
        Terminated              -> HasTerminated
- on_error:
        AwaitUpstream           -> OnError
        AwaitDownstream         -> OnError
        Terminated              -> HasTerminated
- cancel:
        AwaitUpstream           -> Cancelled
        AwaitDownstream         -> CancelledAwaitRequest
        Terminated              -> Cancelled
"""


class MergeStateTransition(ABC):
    @abstractmethod
    def get_state(self) -> MergeState: ...


@dataclass(frozen=True)
class ToStateTransition(MergeStateTransition):
    """Transitions to predefined state"""

    state: MergeState

    def get_state(self):
        return self.state


@dataclass
class InactiveTransitionsMixin:
    id: int

    def _get_state(self, state: MergeState):
        match state:
            case TerminatedStateMixin(
                certificates=certificates,
            ):
                return HasTerminatedState(
                    certificate=certificates[self.id],
                    certificates={
                        id: c for id, c in certificates.items() if id != self.id
                    },
                )

            case _:
                raise Exception(f"Unexpected state {state}.")


@dataclassabc
class OnNextTransition[U](InactiveTransitionsMixin, MergeStateTransition):
    child: MergeStateTransition
    id: int
    value: U
    observer: DeferredHandler

    def get_state(self):
        match state := self.child.get_state():
            case AwaitUpstreamStateMixin(
                active_ids=active_ids,
                certificates=certificates,
            ):
                return OnNextState(
                    value=self.value,
                    observer=self.observer,
                    on_next_calls=tuple(),
                    active_ids=active_ids,
                    certificates=certificates,
                )

            case AwaitDownstreamStateMixin(
                on_next_calls=on_next_calls,
                active_ids=active_ids,
                certificates=certificates,
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
                )

            case _:
                # print(self.child)
                return self._get_state(state)


@dataclassabc
class RequestTransition(MergeStateTransition):
    """Downstream request received"""

    id: int
    child: MergeStateTransition
    certificate: ContinuationCertificate | None

    def get_state(self):
        match previous_state := self.child.get_state():
            case AwaitDownstreamStateMixin(
                on_next_calls=on_next_calls,
                active_ids=active_ids,
                certificates=certificates,
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
                            if len(active_ids) == 0:
                                return OnNextAndCompleteState(
                                    certificates={},
                                    value=value,
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
                            )

                    case _:
                        # no backpressured upstream exist

                        if self.certificate:
                            return AwaitOnNextState(
                                active_ids=active_ids,
                                certificate=self.certificate,
                                certificates=certificates,
                            )

                        else:
                            return AwaitOnNextState(
                                active_ids=active_ids,
                                certificate=certificates[0],
                                certificates=certificates[1:],
                            )

            case CancelledAwaitRequestState(
                certificates=certificates,
                certificate=certificate,
            ):
                if self.certificate:
                    return CancelledStopRequestState(
                        certificate=certificate,
                        certificates={self.id: self.certificate} | certificates,
                    )
                else:
                    return CancelledStopRequestState(
                        certificate=certificate,
                        certificates=certificates,
                    )

            case _:
                raise Exception(f"Unexpected state {previous_state}.")


@dataclassabc
class OnNextAndCompleteTransition[U](InactiveTransitionsMixin, MergeStateTransition):
    child: MergeStateTransition
    id: int
    value: U

    def get_state(self):
        match state := self.child.get_state():
            case AwaitUpstreamStateMixin(
                active_ids=active_ids,
                certificates=certificates,
            ):
                if len(active_ids) == 1:
                    return OnNextAndCompleteState(
                        value=self.value,
                        certificates={},
                    )

                else:
                    return OnNextState(
                        value=self.value,
                        observer=None,
                        on_next_calls=tuple(),
                        active_ids=tuple(id for id in active_ids if id != self.id),
                        certificates=certificates,
                    )

            case AwaitDownstreamStateMixin(
                on_next_calls=on_next_calls,
                active_ids=active_ids,
                certificates=certificates,
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
                )

            case _:
                return self._get_state(state)


@dataclassabc
class OnCompletedTransition(InactiveTransitionsMixin, MergeStateTransition):
    child: MergeStateTransition
    id: int

    def get_state(self):
        match state := self.child.get_state():
            case AwaitUpstreamStateMixin(
                active_ids=active_ids,
                certificates=certificates,
            ):
                if len(active_ids) == 1:
                    return OnCompletedState(certificates={})

                else:
                    return AwaitOnNextState(
                        active_ids=tuple(id for id in active_ids if id != self.id),
                        certificate=certificates[0],
                        certificates=certificates[1:],
                    )

            case AwaitDownstreamStateMixin(
                on_next_calls=on_next_calls,
                active_ids=active_ids,
                certificates=certificates,
            ):
                return KeepWaitingState(
                    on_next_calls=on_next_calls,
                    active_ids=tuple(id for id in active_ids if id != self.id),
                    certificate=certificates[0],
                    certificates=certificates[1:],
                )

            case _:
                return self._get_state(state)


@dataclassabc(frozen=False)
class OnErrorTransition(InactiveTransitionsMixin, MergeStateTransition):
    child: MergeStateTransition
    id: int
    exception: Exception

    def get_state(self):
        match state := self.child.get_state():
            case AwaitUpstreamStateMixin(
                certificates=certificates,
                n_children=n_children,
            ):
                awaiting_ids = tuple(range(n_children))

                return OnErrorState(
                    exception=self.exception,
                    certificates=dict(zip(awaiting_ids, certificates)),
                )

            case AwaitDownstreamStateMixin(
                on_next_calls=on_next_calls,
                certificates=certificates,
                n_children=n_children,
            ):
                received_ids = tuple(call.id for call in on_next_calls)
                awaiting_ids = tuple(
                    id for id in range(n_children) if id not in received_ids
                )
                return OnErrorState(
                    exception=self.exception,
                    certificates=dict(zip(awaiting_ids, certificates)),
                )

            case _:
                return self._get_state(state)


@dataclassabc(frozen=False)
class CancelTransition(MergeStateTransition):
    child: MergeStateTransition
    certificate: ContinuationCertificate

    def get_state(self):
        match child_state := self.child.get_state():
            case AwaitUpstreamStateMixin(
                certificates=certificates,
                n_children=n_children,
            ):
                awaiting_ids = tuple(range(n_children))
                certificates = certificates + (self.certificate,)

                return CancelledState(
                    certificates=dict(zip(awaiting_ids, certificates)),
                )

            case AwaitDownstreamStateMixin(
                on_next_calls=on_next_calls,
                certificates=certificates,
                n_children=n_children,
            ):
                received_ids = tuple(call.id for call in on_next_calls)
                awaiting_ids = tuple(
                    id for id in range(n_children) if id not in received_ids
                )
                return CancelledAwaitRequestState(
                    certificate=self.certificate,
                    certificates=dict(zip(awaiting_ids, certificates)),
                )

            case TerminatedStateMixin(certificates=certificates):
                return CancelledState(certificates=certificates)

            case _:
                raise Exception(f"Unexpected state {child_state}.")
