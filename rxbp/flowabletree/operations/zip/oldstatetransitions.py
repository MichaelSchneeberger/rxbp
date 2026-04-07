from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass

from typing import override

from rxbp.flowabletree.operations.zip.states import (
    AwaitUpstream,
    StopContinuation,
    Completed,
    Completed,
    CancelledUpstreamPending,
    Cancelled,
    Completed,
    Terminated,
    AwaitRequest,
    Active,
    ZipState,
    Error,
)


class ZipStateTransition(ABC):
    @abstractmethod
    def get_state(self) -> ZipState: ...


@dataclass(frozen=True)
class ToStateTransition(ZipStateTransition):
    """Transitions to predefined state"""

    state: ZipState

    @override
    def get_state(self):
        return self.state


@dataclass
class OnNextTransition(ZipStateTransition):
    child: ZipStateTransition
    id: int

    @override
    def get_state(self) -> ZipState:
        match p_state := self.child.get_state():
            # case UpstreamPending():
            #     return p_state.remove(self.id, completed=True)
            #
            # # Flowable got cancelled; stop continuation
            # case CancelledUpstreamPending():
            #     return p_state.remove(self.id)
            #
            # case _:
            #     return Error(f"Unexpected state {p_state}")

            case AwaitUpstream(
                completed_ids=completed_ids,
                requested_ids=requested_ids,
            ):
                # Remove current upstream from the requested upstream IDs
                n_requested_ids = requested_ids - {self.id}

                if len(n_requested_ids):

                    return AwaitUpstream(
                        requested_ids=n_requested_ids,
                        completed_ids=completed_ids,
                    )

                # All requested items received
                else:
                    return AwaitRequest(
                        completed_ids=completed_ids,
                    )

            case Completed(
                requested_ids=requested_ids,
            ):
                n_requested_ids = {id: idx for idx, id in enumerate(requested_ids) if id != self.id}

                return StopContinuation(
                    requested_ids=n_requested_ids,
                    index=self.id,
                )

                # # All requested items received
                # else:
                #     return Completed(
                #         complete=False,
                #     )

            # Flowable got cancelled; stop continuation
            case CancelledUpstreamPending(
                requested_ids=requested_ids,
            ):
                # return p_state.remove(self.id)

                if self.id not in requested_ids:
                    return Error(f'{requested_ids} does not contain {self.id}')

                # remove current upstream ID
                n_requested_ids = {id: idx for id, idx in requested_ids.items() if id != self.id}

                # if n_requested_ids:
                return StopContinuation(
                    requested_ids=n_requested_ids,
                    index=requested_ids[self.id],
                )

                # else:
                #     return Terminated()

            case _:
                return Error(f"Unexpected state {p_state}")


@dataclass
class OnNextAndCompleteTransition[U](ZipStateTransition):
    child: ZipStateTransition
    id: int

    @override
    def get_state(self):
        match p_state := self.child.get_state():
            # case UpstreamPending():
            #     return p_state.remove(self.id, completed=True)
            #
            # # Flowable got cancelled; stop continuation
            # case CancelledUpstreamPending():
            #     return p_state.remove(self.id)
            #
            # case _:
            #     return Error(f"Unexpected state {p_state}")
            case Active(
                completed_ids=completed_ids,
                requested_ids=requested_ids,
            ):
                # Remove current upstream from the requested upstream IDs
                n_requested_ids = requested_ids - {self.id}
                n_completed_ids = completed_ids | {self.id}

                # if len(n_requested_ids):

                return Active(
                    requested_ids=n_requested_ids,
                    completed_ids=n_completed_ids,
                )

                # # All requested items received
                # else:
                #     return AwaitRequest(
                #         completed_ids=n_completed_ids,
                #     )

            case Completed(
                assigned_ids=assigned_ids,
            ):
                # # Remove current upstream from the requested upstream IDs
                # n_requested_ids = requested_ids - {self.id}

                # if len(n_requested_ids):

                n_assigned_ids = {id: idx for idx, id in assigned_ids.items() if id != self.id}

                return StopContinuation(
                    assigned_ids=n_assigned_ids,
                    index=self.id,
                )

                # # All requested items received
                # else:
                #     return Completed(
                #         complete=False,
                #     )

            # # Flowable got cancelled; stop continuation
            # case CancelledUpstreamPending(
            #     requested_ids=requested_ids,
            # ):
            #     # remove current upstream ID
            #     n_requested_ids = {id: idx for id, idx in requested_ids.items() if id != self.id}
            #
            #     # if n_requested_ids:
            #     return StopContinuation(
            #         requested_ids=n_requested_ids,
            #         index=requested_ids[self.id],
            #     )

                # else:
                #     return Terminated()

            case _:
                return Error(f"Unexpected state {p_state}")


@dataclass
class OnCompletedTransition(ZipStateTransition):
    child: ZipStateTransition
    id: int

    @override
    def get_state(self):
        match p_state := self.child.get_state():
            case AwaitUpstream(
                requested_ids=requested_ids,
            ):
                n_requested_ids = requested_ids - {self.id}
    
                # if n_requested_ids:
                return Completed(
                    requested_ids=n_requested_ids,
                    # complete_downstream=True,
                )

                # else:
                #     return Completed(
                #         complete=True,
                #     )

            case Completed(
                requested_ids=requested_ids,
            ):
                # n_requested_ids = requested_ids - {self.id}
                n_requested_ids = {id: idx for idx, id in enumerate(requested_ids) if id != self.id}

                return StopContinuation(
                    requested_ids=n_requested_ids,
                    index=self.id,
                )

                # if n_requested_ids:
                # return Completed(
                #     requested_ids=n_requested_ids,
                #     complete_downstream=False,
                # )

                # else:
                #     return Completed(
                #         complete=False,
                #     )

            # Flowable got cancelled; stop continuation
            case CancelledUpstreamPending(
                requested_ids=requested_ids,
            ):
                # remove current upstream ID
                n_requested_ids = {id: idx for id, idx in requested_ids.items() if id != self.id}

                # if n_requested_ids:
                return StopContinuation(
                    requested_ids=n_requested_ids,
                    index=requested_ids[self.id],
                )

                # else:
                #     return Terminated()

            case _:
                return Error(f"Unexpected state {p_state}")


# @dataclass
# class OnErrorTransition(ZipStateTransition):
#     child: ZipStateTransition
#     id: int
#
#     @override
#     def get_state(self):
#         match p_state := self.child.get_state():
#             case AwaitUpstream(
#                 requested_ids=requested_ids,
#             ):
#                 n_requested_ids = requested_ids - {self.id}
#
#                 return OnError(
#                     requested_ids=n_requested_ids,
#                 )
#
#             case _:
#                 return Error(f"Unexpected state {p_state}")


@dataclass
class RequestTransition(ZipStateTransition):
    """Downstream requests new item"""

    child: ZipStateTransition
    requested_ids: set[int]

    @override
    def get_state(self):
        match p_state := self.child.get_state():
            case AwaitRequest(completed_ids=completed_ids):
                return AwaitUpstream(
                    requested_ids=self.requested_ids,
                    completed_ids=completed_ids,
                )

            case Cancelled():
                return Terminated()

            case _:
                return Error(f"Unexpected state {p_state}.")


@dataclass
class CancelTransition(ZipStateTransition):
    child: ZipStateTransition
    # certificate: ContCert

    @override
    def get_state(self):
        match p_state := self.child.get_state():
            case Active(
                requested_ids=requested_ids,
            ):
                n_requested_ids = {id: idx for idx, id in enumerate(requested_ids)}

                return CancelledUpstreamPending(
                    requested_ids=n_requested_ids,
                )

            case AwaitRequest():
                return Cancelled()

            # case Completed():
            #     return p_state

            case _:
                return Error(f"Unexpected state {p_state}.")

