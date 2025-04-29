from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass

from dataclassabc import dataclassabc

from continuationmonad.typing import (
    ContinuationCertificate,
    DeferredHandler,
)

from rxbp.flowabletree.operations.merge.states import (
    AwaitDownstreamStateMixin,
    AwaitUpstreamStateMixin,
    CancelledStopRequestState,
    InitState,
    CancelledAwaitRequestState,
    CancelledState,
    HasTerminatedState,
    MergeState,
    OnCompletedState,
    OnErrorState,
    TerminatedStateMixin,
    UpstreamID,
    OnNextState,
    KeepWaitingState,
    AwaitOnNextState,
    BackpressuredOnNextCalls,
    OnNextAndCompleteState,
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
    id: UpstreamID
    value: U
    observer: DeferredHandler

    def get_state(self):
        match state := self.child.get_state():
            case AwaitUpstreamStateMixin(
                n_completed=n_completed,
                certificates=certificates,
            ):
                return OnNextState(
                    value=self.value,
                    observer=self.observer,
                    on_next_calls=tuple(),
                    n_completed=n_completed,
                    certificates=certificates,
                )

            case AwaitDownstreamStateMixin(
                on_next_calls=on_next_calls,
                n_completed=n_completed,
                certificates=certificates,
            ):
                # backpressure on_next call

                pre_state = BackpressuredOnNextCalls(
                    id=self.id,
                    value=self.value,
                    observer=self.observer,
                    # completed=False,
                )
                n_on_next_calls = on_next_calls + (pre_state,)

                return KeepWaitingState(
                    n_completed=n_completed,
                    on_next_calls=n_on_next_calls,
                    certificate=certificates[0],
                    certificates=certificates[1:],
                )

            case _:
                return self._get_state(state)


@dataclassabc
class OnNextAndCompleteTransition[U](InactiveTransitionsMixin, MergeStateTransition):
    child: MergeStateTransition
    id: UpstreamID
    value: U
    n_children: int

    def get_state(self):
        match state := self.child.get_state():
            case AwaitUpstreamStateMixin(
                n_completed=n_completed,
                certificates=certificates,
            ):
                if self.n_children == n_completed + 1:
                    return OnNextAndCompleteState(
                        value=self.value,
                        certificates={},
                    )

                else:
                    return OnNextState(
                        value=self.value,
                        observer=None,
                        on_next_calls=tuple(),
                        # certificate=certificates[0],
                        n_completed=n_completed + 1,
                        certificates=certificates,
                    )

            case AwaitDownstreamStateMixin(
                on_next_calls=on_next_calls,
                n_completed=n_completed,
                certificates=certificates,
            ):
                # backpressure on_next call

                pre_state = BackpressuredOnNextCalls(
                    id=self.id,
                    value=self.value,
                    observer=None,
                    # completed=True,
                )
                n_on_next_calls = on_next_calls + (pre_state,)

                return KeepWaitingState(
                    n_completed=n_completed,
                    on_next_calls=n_on_next_calls,
                    certificate=certificates[0],
                    certificates=certificates[1:],
                )

            case _:
                return self._get_state(state)


@dataclassabc
class RequestTransition(MergeStateTransition):
    """Downstream request received"""

    id: UpstreamID
    child: MergeStateTransition
    certificate: ContinuationCertificate | None
    n_children: int

    def get_state(self):
        match previous_state := self.child.get_state():
            case AwaitDownstreamStateMixin(
                on_next_calls=on_next_calls,
                n_completed=n_completed,
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
                        # backpressure on_next call exist

                        if observer is None:
                            # is on_next_and_complete

                            # last element in buffer
                            if len(others) == 0 and self.n_children == n_completed + 1:
                                return OnNextAndCompleteState(
                                    certificates={},
                                    value=value,
                                )

                            else:
                                return OnNextState(
                                    observer=None,
                                    value=value,
                                    on_next_calls=tuple(others),
                                    n_completed=n_completed,
                                    certificates=certificates + (self.certificate,)
                                    if self.certificate
                                    else certificates,
                                )

                        else:
                            return OnNextState(
                                value=value,
                                observer=observer,
                                on_next_calls=tuple(others),
                                n_completed=n_completed,
                                certificates=certificates + (self.certificate,)
                                if self.certificate
                                else certificates,
                            )

                    case _:
                        # no backpressured on_next call exist

                        if self.certificate:
                            return AwaitOnNextState(
                                n_completed=n_completed,
                                certificate=self.certificate,
                                certificates=certificates,
                            )

                        else:
                            return AwaitOnNextState(
                                n_completed=n_completed,
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
class OnCompletedTransition(InactiveTransitionsMixin, MergeStateTransition):
    child: MergeStateTransition
    id: UpstreamID
    n_children: int

    def get_state(self):
        match state := self.child.get_state():
            case AwaitUpstreamStateMixin(
                n_completed=n_completed,
                certificates=certificates,
            ):
                if self.n_children == n_completed + 1:
                    return OnCompletedState(certificates={})

                else:
                    return AwaitOnNextState(
                        n_completed=n_completed + 1,
                        certificate=certificates[0],
                        certificates=certificates[1:],
                    )

            case AwaitDownstreamStateMixin(
                on_next_calls=on_next_calls,
                n_completed=n_completed,
                certificates=certificates,
            ):
                return KeepWaitingState(
                    on_next_calls=on_next_calls,
                    n_completed=n_completed + 1,
                    certificate=certificates[0],
                    certificates=certificates[1:],
                )

            case _:
                return self._get_state(state)


@dataclassabc(frozen=False)
class OnErrorTransition(InactiveTransitionsMixin, MergeStateTransition):
    child: MergeStateTransition
    id: UpstreamID
    n_children: int
    exception: Exception

    def get_state(self):
        match state := self.child.get_state():
            case AwaitUpstreamStateMixin(certificates=certificates):
                awaiting_ids = tuple(range(self.n_children))

                return OnErrorState(
                    exception=self.exception,
                    certificates=dict(zip(awaiting_ids, certificates)),
                )

            case AwaitDownstreamStateMixin(
                on_next_calls=on_next_calls, certificates=certificates
            ):
                received_ids = tuple(call.id for call in on_next_calls)
                awaiting_ids = tuple(
                    id for id in range(self.n_children) if id not in received_ids
                )
                return OnErrorState(
                    exception=self.exception,
                    certificates=dict(zip(awaiting_ids, certificates)),
                    # awaiting_ids=awaiting_ids,
                )

            case _:
                return self._get_state(state)


@dataclassabc(frozen=False)
class CancelTransition(MergeStateTransition):
    child: MergeStateTransition
    n_children: int
    certificate: ContinuationCertificate

    def get_state(self):
        match child_state := self.child.get_state():
            case AwaitUpstreamStateMixin(
                # on_next_calls=on_next_calls,
                certificates=certificates,
            ):
                awaiting_ids = tuple(range(self.n_children))
                certificates = certificates + (self.certificate,)

                return CancelledState(
                    certificates=dict(zip(awaiting_ids, certificates)),
                )

            case AwaitDownstreamStateMixin(
                on_next_calls=on_next_calls,
                certificates=certificates,
            ):
                received_ids = tuple(call.id for call in on_next_calls)
                awaiting_ids = tuple(
                    id for id in range(self.n_children) if id not in received_ids
                )
                return CancelledAwaitRequestState(
                    certificate=self.certificate,
                    certificates=dict(zip(awaiting_ids, certificates)),
                )

            case TerminatedStateMixin(certificates=certificates):
                return CancelledState(certificates=certificates)

            case _:
                raise Exception(f"Unexpected state {child_state}.")

        # return CancelledState(
        #     certificates=certificates,
        # )


@dataclassabc(frozen=False)
class SubscribeTransition(MergeStateTransition):
    id: UpstreamID
    child: MergeStateTransition
    certificate: ContinuationCertificate

    def get_state(self):
        match state := self.child.get_state():
            case AwaitDownstreamStateMixin():
                return AwaitDownstreamStateMixin(
                    n_completed=state.n_completed,
                    certificates=state.certificates + (self.certificate,),
                    on_next_calls=state.on_next_calls,
                )

            case AwaitItemsStateMixin():
                return InitState(
                    n_completed=state.n_completed,
                    certificates=state.certificates + (self.certificate,),
                )

            case TerminatedStateMixin(
                certificates=certificates, awaiting_ids=awaiting_ids
            ):
                certificates = certificates + (self.certificate,)

                return TerminatedStateMixin(
                    certificates=certificates, awaiting_ids=awaiting_ids + (self.id,)
                )

            case CompletedBaseState():
                return CompletedBaseState()

            case CancelledState(certificates=certificates):
                return CancelledState(certificates | {self.id: self.certificate})

            case _:
                raise Exception(f"Unexpected state {state}.")
