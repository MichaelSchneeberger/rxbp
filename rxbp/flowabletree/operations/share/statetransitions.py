from abc import ABC, abstractmethod
from dataclasses import replace

from dataclassabc import dataclassabc

from continuationmonad.typing import ContinuationCertificate

from rxbp.flowabletree.operations.share.states import (
    AwaitUpstreamAdjustBufferState,
    ErrorStateMixin,
    HasErroredState,
    HasTerminatedState,
    AwaitDownstreamAdjustBufferState,
    CancelledKeepAwaitDownstreamState,
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

State groups (-), states (>):
- Active - AwaitUpstream
  > Init
  > RequestUpstream
  > AwaitOnNext
  > OnNextFromBuffer
- Active - AwaitDownstream
  > OnNext
- Terminated
  > OnCompleted
  > Cancelled
  > HasTerminated
- Terminated - Errored
  > OnError
  > HasErrored

Transitions:
- on_next:
        AwaitUpstream           -> OnNext
        Cancelled               -> HasTerminated
- request:
        Active                  -> RequestUpstream          if first request
                                -> AwaitOnNext              if item is requested but not yet reveived
                                -> OnNextFromBuffer
        Error                   -> HasErrored



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
            case AwaitUpstreamStateMixin(
                # buffer_map=buffer_map,
                # first_buffer_index=first_buffer_index,
                # last_buffer_index=last_buffer_index,
                # requested_certificates=requested_certificates,
            ):
                last_buffer_index = state.last_buffer_index + 1

                send_ids = tuple(
                    id
                    for id, index in state.buffer_map.items()
                    if index == last_buffer_index
                )

                # not all subscribers request new item
                buffer_item = state.first_buffer_index != last_buffer_index

                return OnNext(
                    buffer_map=state.buffer_map,
                    first_buffer_index=state.first_buffer_index,
                    last_buffer_index=last_buffer_index,
                    weights=state.weights,
                    requested_certificates=state.requested_certificates,
                    send_ids=send_ids,
                    buffer_item=buffer_item,
                    is_completed=False,
                    cancelled_certificates=state.cancelled_certificates,
                )

            case CancelledState(requested_certificates=requested_certificates):
                return HasTerminatedState(requested_certificates=requested_certificates)

            case _:
                raise Exception(
                    f"Unexpected state {state} returned by action {self.child}."
                )


@dataclassabc
class RequestTransition(ShareStateTransition):
    """Downstream request received"""

    child: ShareStateTransition
    id: int  # downstream id
    weight: int  # downstream weight

    def get_state(self):
        match state := self.child.get_state():
            case ActiveStateMixin():

                def gen_weights():
                    for id, w in state.weights.items():
                        if id == self.id:
                            yield id, self.weight
                        else:
                            yield id, w
            
                weights = dict(gen_weights())

                if self.id in state.cancelled_certificates:
                    return CancelledKeepAwaitDownstreamState(
                        buffer_map=state.buffer_map,
                        first_buffer_index=state.first_buffer_index,
                        last_buffer_index=state.last_buffer_index,
                        weights=weights,
                        requested_certificates=state.requested_certificates,
                        certificate=state.cancelled_certificates[self.id],
                        is_completed=state.is_completed,
                        cancelled_certificates=state.cancelled_certificates,
                    )

                # update buffer_map
                buffer_map = {}
                for id, index in state.buffer_map.items():
                    if id == self.id:
                        sel_index = index
                        buffer_map[id] = index + 1

                    else:
                        buffer_map[id] = index

                if state.last_buffer_index == sel_index:
                    """ there is no item in the buffer """

                    if isinstance(state, AwaitDownstreamStateMixin):
                        """ request new upstream item """

                        return RequestUpstream(
                            buffer_map=buffer_map,
                            first_buffer_index=state.first_buffer_index,
                            last_buffer_index=state.last_buffer_index,
                            weights=weights,
                            requested_certificates=state.requested_certificates,
                            # certificate=None,
                            is_completed=state.is_completed,
                            cancelled_certificates=state.cancelled_certificates,
                        )

                    else:
                        certificate = state.requested_certificates[self.id]
                        # requested_certificates = {id: c for id, c in requested_certificates.items() if id != self.id}

                        return AwaitOnNext(
                            buffer_map=buffer_map,
                            first_buffer_index=state.first_buffer_index,
                            last_buffer_index=state.last_buffer_index,
                            weights=weights,
                            requested_certificates=state.requested_certificates,
                            certificate=certificate,
                            is_completed=state.is_completed,
                            cancelled_certificates=state.cancelled_certificates,
                        )

                else:
                    """ there is an item in the buffer """

                    if sel_index == state.first_buffer_index:
                        # update first index

                        n_observers = sum(
                            index
                            for index in buffer_map.values()
                            if index == state.first_buffer_index
                        )

                        # no upstream points to first index
                        if pop_item := n_observers == 0:
                            first_buffer_index = state.first_buffer_index + 1

                    else:
                        pop_item = False

                    return OnNextFromBuffer(
                        buffer_map=buffer_map,
                        first_buffer_index=first_buffer_index,
                        last_buffer_index=state.last_buffer_index,
                        weights=weights,
                        requested_certificates=state.requested_certificates,
                        is_completed=state.is_completed,
                        index=sel_index,
                        pop_item=pop_item,
                        cancelled_certificates=state.cancelled_certificates,
                    )
            
            case ErrorStateMixin(
                exception=exception,
                requested_certificates=requested_certificates,
            ):
                # send on_error

                return HasErroredState(
                    exception=exception,
                    requested_certificates=requested_certificates,
                )

            case TerminatedStateMixin(requested_certificates=requested_certificates):
                return HasTerminatedState(requested_certificates=requested_certificates)

            case _:
                raise Exception(f"Unexpected state {state}.")


@dataclassabc(frozen=False)
class OnNextAndCompleteTransition(ShareStateTransition):
    """Item received, stream is complete"""
    child: ShareStateTransition

    def get_state(self):
        match state := self.child.get_state():
            case AwaitUpstreamStateMixin(
                buffer_map=buffer_map,
                first_buffer_index=first_buffer_index,
                last_buffer_index=last_buffer_index,
                # is_ack=is_ack,
                requested_certificates=requested_certificates,
            ):
                n_last_buffer_index = last_buffer_index + 1

                send_ids = tuple(
                    id
                    for id, index in buffer_map.items()
                    if index == n_last_buffer_index
                )

                buffer_item = first_buffer_index != n_last_buffer_index      # all subscribers request new item

                return OnNext(
                    buffer_map=buffer_map,
                    first_buffer_index=first_buffer_index,
                    last_buffer_index=n_last_buffer_index,
                    weights=state.weights,
                    requested_certificates=requested_certificates,
                    send_ids=send_ids,
                    buffer_item=buffer_item,
                    is_completed=True,
                    cancelled_certificates=state.cancelled_certificates,
                )

            case CancelledState(requested_certificates=requested_certificates):
                return HasTerminatedState(requested_certificates=requested_certificates)

            case _:
                raise Exception(f"Unexpected state {state}.")


@dataclassabc(frozen=False)
class OnCompletedTransition(ShareStateTransition):
    """Stream is complete"""

    child: ShareStateTransition

    def get_state(self):
        match state := self.child.get_state():
            case ActiveStateMixin(
                buffer_map=buffer_map,
                last_buffer_index=last_buffer_index,
                requested_certificates=requested_certificates,
            ):
                send_ids = tuple(
                    id for id, index in buffer_map.items() if last_buffer_index <= index
                )

                return OnCompletedState(
                    send_ids=send_ids,
                    requested_certificates=requested_certificates,
                )
            
            case CancelledState(requested_certificates=requested_certificates):
                return HasTerminatedState(requested_certificates=requested_certificates)

            case _:
                raise Exception(f"Unexpected state {state}.")


@dataclassabc(frozen=False)
class OnErrorTransition(ShareStateTransition):
    child: ShareStateTransition
    exception: Exception

    def get_state(self):
        match state := self.child.get_state():
            case ActiveStateMixin(
                buffer_map=buffer_map,
                last_buffer_index=last_buffer_index,
                requested_certificates=requested_certificates,
            ):
                send_ids = tuple(
                    id for id, index in buffer_map.items() if index == last_buffer_index
                )

                return OnErrorState(
                    exception=self.exception,
                    send_ids=send_ids,
                    requested_certificates=requested_certificates,
                )

            case CancelledState(requested_certificates=requested_certificates):
                return HasTerminatedState(requested_certificates=requested_certificates)

            case _:
                raise Exception(f"Unexpected state {state}.")


@dataclassabc(frozen=False)
class CancelTransition(ShareStateTransition):
    child: ShareStateTransition
    id: int
    certificate: ContinuationCertificate

    def get_state(self):
        match state := self.child.get_state():
            case ActiveStateMixin(
                buffer_map=buffer_map,
                requested_certificates=requested_certificates,
                first_buffer_index=first_buffer_index,
            ):
                # remove downstrem from buffer
                n_buffer_map = {
                    id: index for id, index in buffer_map.items() if id != self.id
                }

                certificate = requested_certificates[self.id].or_(self.certificate)

                if len(n_buffer_map) == 0:
                    # no active downstream observers

                    requested_certificates = requested_certificates | {self.id: certificate}

                    return CancelledState(
                        requested_certificates=requested_certificates,
                    )
                
                else:
                    first_buffer_index = min(n_buffer_map.values())
                    n_buffer_pop = first_buffer_index - first_buffer_index

                    match state:
                        case AwaitUpstreamStateMixin():
                            requested_certificates = requested_certificates | {self.id: certificate}

                            return AwaitUpstreamAdjustBufferState(
                                buffer_map=n_buffer_map,
                                first_buffer_index=first_buffer_index,
                                last_buffer_index=state.last_buffer_index,
                                weights=state.weights,
                                requested_certificates=requested_certificates,
                                is_completed=state.is_completed,
                                n_buffer_pop=n_buffer_pop,
                                cancelled_certificates=state.cancelled_certificates,
                            )

                        case AwaitDownstreamStateMixin(
                            cancelled_certificates=cancelled_certificates,
                        ):
                            cancelled_certificates = cancelled_certificates | {self.id: certificate}
                            requested_certificates = {id: c for id, c in requested_certificates.items() if id != self.id}

                            return AwaitDownstreamAdjustBufferState(
                                buffer_map=n_buffer_map,
                                first_buffer_index=first_buffer_index,
                                last_buffer_index=state.last_buffer_index,
                                weights=state.weights,
                                requested_certificates=requested_certificates,
                                is_completed=state.is_completed,
                                n_buffer_pop=n_buffer_pop,
                                cancelled_certificates=cancelled_certificates,
                            )

            case TerminatedStateMixin(requested_certificates=requested_certificates):
                return HasTerminatedState(requested_certificates=requested_certificates)

            case _:
                raise Exception(f"Unexpected state {state}.")
