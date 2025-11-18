from abc import ABC, abstractmethod
from dataclasses import replace
from threading import Lock
from continuationmonad.scheduler.continuationcertificate import (
    ContinuationCertificateImpl,
)
from continuationmonad.utils.framesummary import get_frame_summary
from dataclassabc import dataclassabc

from continuationmonad.typing import ContinuationCertificate

from rxbp.flowabletree.operations.asyncshare.states import (
    ActiveState,
    AwaitDownstreamStateMixin,
    CancelledState,
    OnCompletedState,
    OnErrorState,
    OnNext,
    ShareState,
    TerminatedState,
)

"""
State Machine
-------------

The states are described in states.py

Transitions:
- on_next:
        AwaitUpstream           -> OnNext
        Cancelled               -> HasTerminated
- request:
        Active                  -> RequestUpstream          if first request
                                -> AwaitOnNext              if item is requested but not yet reveived
                                -> OnNextFromBuffer
        Error                   -> Error



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


class ShareStateTransition(ABC):
    @abstractmethod
    def get_state(self) -> ShareState: ...


@dataclassabc(frozen=True)
class ToStateTransition(ShareStateTransition):
    """Transitions to predefined state"""

    state: ShareState

    def get_state(self):
        return self.state


@dataclassabc(frozen=True)
class SubscriptionTransition(ShareStateTransition):
    child: ShareStateTransition
    weight: int

    def get_state(self):
        match state := self.child.get_state():

            case ActiveState():
                id = state.subscription_cnt + 1

                # first subscription
                if id == 1:

                    buffer_map = state.buffer_map
                    return state.copy(
                        action=ActiveState.Subscribe(),
                        buffer_map=buffer_map.copy(

                            # initialize the item index for the subscribed observer
                            mapping=buffer_map.mapping | {id: buffer_map.next_index},
                        ),
                    )

                # trampoline the subsequent subscriptions before sending first items downstream
                else:
                    return state.copy(
                        action=ActiveState.TrampolineSubscription()
,
                    )

            case TerminatedState():
                """
                If the share observable was completed or errored, schedule a task that completes or errors
                the downstream observer.
                If the share observable was disposed, ignore subscription.
                """

                raise NotImplementedError


@dataclassabc(frozen=True)
class TrampolineSubscriptionTransition(ShareStateTransition):
    child: ShareStateTransition
    id: int

    def get_state(self):
        match state := self.child.get_state():
            case ActiveState():
                return state.copy(
                    action=ActiveState.ActivateSubscription(),
                )

            case TerminatedState():
                raise NotImplementedError


@dataclassabc
class RequestTransition(ShareStateTransition):
    """
    Downstream request received
    """

    child: ShareStateTransition
    id: int  # downstream id

    def get_state(self):
        match state := self.child.get_state():
            case ActiveState():

                # increment the position within the buffer
                pop_buffer, buffer_map = state.buffer_map.increment(self.id)

                # downstream is waiting for the next upstream item
                if buffer_map.is_waiting(self.id):

                    # corresponds to the first observer that requests a new item
                    if not state.buffer_map.is_any_downstream_waiting():
                        return state.copy(
                            action=ActiveState.RequestNextItem(
                                pop_buffer=pop_buffer,
                            ),
                            buffer_map=buffer_map,
                        )

                    # corresponds to a subsequent requests
                    else:
                        certificate = state.get_downstream_certificate(self.id)

                        return state.copy(
                            action=ActiveState.AwaitUpstream(
                                certificate=certificate,
                            ),
                            buffer_map=buffer_map,
                        )

                # send an item from buffer
                else:
                    return state.copy(
                        action=ActiveState.OnNextFromBuffer(),
                        buffer_map=buffer_map,
                    )

            case OnCompletedState() | OnErrorState():
                return state

            case _:
                raise Exception(f"Unexpected state {state}.")


@dataclassabc(frozen=False)
class OnNextTransition(ShareStateTransition):
    """Next upstream item received"""

    child: ShareStateTransition

    def get_state(self):
        match state := self.child.get_state():
            case ActiveState(is_awaiting_upstream=True):
                assert not state.is_completed, (
                    "Share Observer received item after complete."
                )

                buffer_map = state.buffer_map
                buffer_map=buffer_map.copy(
                    # at this point, the received item has already been added to the buffer
                    # update the buffer_map accordingly
                    next_index=buffer_map.next_index + 1,
                )
                return state.copy(
                    buffer_map=buffer_map,
                    action=ActiveState.OnNext(
                        buffer_map=buffer_map,
                    ),
                )

            case CancelledState():
                return state

            case _:
                raise Exception(
                    f"Unexpected state {state} returned by action {self.child}."
                )


@dataclassabc(frozen=False)
class OnNextAndCompleteTransition(ShareStateTransition):
    """Item received, stream is complete"""

    child: ShareStateTransition

    def get_state(self):
        match state := self.child.get_state():
            case ActiveState(is_awaiting_upstream=True):
                assert not state.is_completed, (
                    "Share Observer received item after complete."
                )

                buffer_map = state.buffer_map
                return state.copy(
                    buffer_map=buffer_map.copy(

                        # at this point, the received item has already been added to the buffer
                        # update the buffer_map accordingly
                        next_index=buffer_map.next_index + 1,
                    ),
                    is_completed=True,
                    action=ActiveState.OnNextAndComplete(
                        # downstream_ids=tuple(state.buffer_map.get_waiting_downstreams()),
                    ),
                )

            case CancelledState():
                return state

            case _:
                raise Exception(
                    f"Unexpected state {state}."
                )


@dataclassabc(frozen=False)
class OnCompletedTransition(ShareStateTransition):
    """Stream is complete"""

    child: ShareStateTransition

    def get_state(self):
        match state := self.child.get_state():
            case ActiveState():
                return state.copy(
                    action=ActiveState.OnCompleted(
                        # downstream_ids=tuple(state.buffer_map.get_waiting_downstreams()),
                    ),
                )

            case CancelledState():
                return state

            case _:
                raise Exception(f"Unexpected state {state}.")


@dataclassabc(frozen=False)
class OnErrorTransition(ShareStateTransition):
    child: ShareStateTransition
    exception: Exception

    def get_state(self):
        match state := self.child.get_state():
            case ActiveState():
                return state.copy(
                    action=ActiveState.OnError(
                        # downstream_ids=tuple(state.buffer_map.get_waiting_downstreams()),
                    ),
                )

            case CancelledState():
                return state

            case _:
                raise Exception(f"Unexpected state {state}.")


# @dataclassabc(frozen=False)
# class CancelTransition(ShareStateTransition):
#     child: ShareStateTransition
#     id: int
#     certificate: ContinuationCertificate
#
#     def get_state(self):
#         match state := self.child.get_state():
#             case ActiveState():
#                 # remove downstream from buffer mapping
#                 buffer_map = {
#                     id: index for id, index in state.buffer_map.items() if id != self.id
#                 }
#
#                 match state:
#                     case AwaitUpstreamStateMixin():
#                         # on_next call has not been yet
#                         # print(f"{state.is_awaiting(self.id)}")
#                         if state.is_awaiting(self.id):
#                             requested_certificates = state.requested_certificates | {
#                                 self.id: self.certificate
#                             }
#
#                         else:
#                             requested_certificates = state.requested_certificates
#
#                         if len(buffer_map) == 0:
#                             certificate = ContinuationCertificate.merge(
#                                 tuple(requested_certificates.values())
#                             )
#
#                             return CancelledState(
#                                 # requested_certificates=requested_certificates,
#                                 certificate=certificate,
#                             )
#
#                         else:
#                             return AwaitUpstreamAfterCancelState(
#                                 weights=state.weights,
#                                 buffer_map=buffer_map,
#                                 first_buffer_index=min(buffer_map.values()),
#                                 last_buffer_index=state.last_buffer_index,
#                                 requested_certificates=requested_certificates,
#                                 is_completed=state.is_completed,
#                             )
#
#                     case AwaitDownstreamStateMixin():
#                         return AwaitDownstreamAfterCancelState(
#                             weights=state.weights,
#                             buffer_map=buffer_map,
#                             first_buffer_index=min(buffer_map.values()),
#                             last_buffer_index=state.last_buffer_index,
#                             requested_certificates=state.requested_certificates,
#                             is_completed=state.is_completed,
#                         )
#
#             case TerminatedState():
#                 return state
#
#             case _:
#                 raise Exception(f"Unexpected state {state}.")
