from abc import ABC, abstractmethod
from dataclasses import replace

from dataclassabc import dataclassabc

from continuationmonad.typing import ContinuationCertificate

from rxbp.flowabletree.operations.share.states import (
    AwaitUpstreamAfterCancelState,
    # ErrorStateMixin,
    # HasErroredState,
    # HasTerminatedState,
    AwaitDownstreamAfterCancelState,
    RequestFromCancelledDownstreamState,
    RequestUpstream,
    ActiveStateMixin,
    AwaitOnNext,
    AwaitUpstreamStateMixin,
    AwaitDownstreamStateMixin,
    CancelledState,
    OnCompletedState,
    OnErrorState,
    OnNextFromBuffer,
    OnNext,
    ShareState,
    TerminatedStateMixin,
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


@dataclassabc(frozen=False)
class OnNextTransition(ShareStateTransition):
    """Item received"""

    child: ShareStateTransition

    def get_state(self):
        match state := self.child.get_state():
            case AwaitUpstreamStateMixin():
                assert not state.is_completed, (
                    "Share Observer is completed while receiving an item."
                )

                return OnNext(
                    buffer_map=state.buffer_map,
                    first_buffer_index=state.first_buffer_index,
                    last_buffer_index=state.last_buffer_index + 1,
                    requested_certificates=state.requested_certificates,
                    is_completed=state.is_completed,
                )

            case CancelledState():
                return state

            case _:
                raise Exception(
                    f"Unexpected state {state} returned by action {self.child}."
                )


@dataclassabc
class RequestTransition(ShareStateTransition):
    """Downstream request received"""

    child: ShareStateTransition
    id: int  # downstream id

    def get_state(self):
        match state := self.child.get_state():
            case ActiveStateMixin():
                # update buffer_map
                buffer_map = {}
                sel_index = None
                for id, index in state.buffer_map.items():
                    if id == self.id:
                        sel_index = index
                        buffer_map[id] = index + 1

                    else:
                        buffer_map[id] = index

                assert sel_index is not None, "id not found in buffer."

                # there is no item in the buffer
                if state.last_buffer_index == sel_index:
                    # request new upstream item
                    if isinstance(state, AwaitDownstreamStateMixin):
                        return RequestUpstream(
                            buffer_map=buffer_map,
                            first_buffer_index=state.first_buffer_index,
                            last_buffer_index=state.last_buffer_index,
                            requested_certificates=state.requested_certificates,
                            is_completed=state.is_completed,
                        )

                    else:
                        certificate = state.requested_certificates[self.id]

                        return AwaitOnNext(
                            buffer_map=buffer_map,
                            first_buffer_index=state.first_buffer_index,
                            last_buffer_index=state.last_buffer_index,
                            requested_certificates=state.requested_certificates,
                            certificate=certificate,
                            is_completed=state.is_completed,
                        )

                # there is an item in the buffer
                else:
                    first_buffer_index = state.first_buffer_index

                    # update first index
                    if sel_index == first_buffer_index:
                        # number of downstream observers pointing to the first buffer item
                        n_observers = sum(
                            index
                            for index in buffer_map.values()
                            if index == state.first_buffer_index
                        )

                        # no upstream observers point to the first buffer item, increase index
                        if pop_item := n_observers == 0:
                            first_buffer_index = first_buffer_index + 1

                    else:
                        pop_item = False

                    return OnNextFromBuffer(
                        buffer_map=buffer_map,
                        first_buffer_index=first_buffer_index,
                        last_buffer_index=state.last_buffer_index,
                        requested_certificates=state.requested_certificates,
                        is_completed=state.is_completed,
                        index=sel_index,
                        pop_item=pop_item,
                    )

            case OnCompletedState() | OnErrorState():
                return state

            case _:
                raise Exception(f"Unexpected state {state}.")


@dataclassabc(frozen=False)
class OnNextAndCompleteTransition(ShareStateTransition):
    """Item received, stream is complete"""

    child: ShareStateTransition

    def get_state(self):
        match state := self.child.get_state():
            case AwaitUpstreamStateMixin():
                return OnNext(
                    buffer_map=state.buffer_map,
                    first_buffer_index=state.first_buffer_index,
                    last_buffer_index=state.last_buffer_index + 1,
                    requested_certificates=state.requested_certificates,
                    is_completed=True,
                )

            case CancelledState():
                return state

            case _:
                raise Exception(f"Unexpected state {state}.")


@dataclassabc(frozen=False)
class OnCompletedTransition(ShareStateTransition):
    """Stream is complete"""

    child: ShareStateTransition

    def get_state(self):
        match state := self.child.get_state():
            case ActiveStateMixin():
                return OnCompletedState(
                    send_ids=state.get_synced_ids(),
                    requested_certificates=state.requested_certificates,
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
            case ActiveStateMixin():
                return OnErrorState(
                    send_ids=state.get_synced_ids(),
                    requested_certificates=state.requested_certificates,
                )

            case CancelledState():
                return state

            case _:
                raise Exception(f"Unexpected state {state}.")


@dataclassabc(frozen=False)
class CancelTransition(ShareStateTransition):
    child: ShareStateTransition
    id: int
    certificate: ContinuationCertificate

    def get_state(self):
        match state := self.child.get_state():
            case ActiveStateMixin():
                # remove downstream from buffer mapping
                buffer_map = {
                    id: index for id, index in state.buffer_map.items() if id != self.id
                }

                match state:
                    case AwaitUpstreamStateMixin():
                        # on_next call has not been yet
                        # print(f"{state.is_awaiting(self.id)}")
                        if state.is_awaiting(self.id):
                            requested_certificates = state.requested_certificates | {
                                self.id: self.certificate
                            }

                        else:
                            requested_certificates = state.requested_certificates

                        if len(buffer_map) == 0:
                            certificate = ContinuationCertificate.merge(
                                tuple(requested_certificates.values())
                            )
                            # raise Exception()

                            return CancelledState(
                                # requested_certificates=requested_certificates,
                                certificate=certificate,
                            )

                        else:
                            return AwaitUpstreamAfterCancelState(
                                buffer_map=buffer_map,
                                first_buffer_index=min(buffer_map.values()),
                                last_buffer_index=state.last_buffer_index,
                                requested_certificates=requested_certificates,
                                is_completed=state.is_completed,
                            )

                    case AwaitDownstreamStateMixin():
                        return AwaitDownstreamAfterCancelState(
                            buffer_map=buffer_map,
                            first_buffer_index=min(buffer_map.values()),
                            last_buffer_index=state.last_buffer_index,
                            requested_certificates=state.requested_certificates,
                            is_completed=state.is_completed,
                        )

            case TerminatedStateMixin():
                return state

            case _:
                raise Exception(f"Unexpected state {state}.")
