from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Self, override


class ZipState: ...


@dataclass(frozen=True)
class Error(ZipState):
    msg: str


@dataclass(frozen=True)
class Active(ZipState, ABC):
    requested_ids: set[int]
    completed_ids: set[int]

    # @abstractmethod
    # def remove(self, id: int, completed: bool) -> ZipState: ...


@dataclass(frozen=True)
class Completed(ZipState, ABC):
    assigned_ids: dict[int, int]


@dataclass(frozen=True, slots=True)
class StopContinuation(Completed):
    """
    Cancel the Flowable.
    """

    index: int


# @dataclass(frozen=True, slots=True)
# class Cancelled(ZipState):
#     """
#     Flowable gets cancelled while downstream request pending.
#     """


# @dataclass(frozen=True)
# class NonTerminated(ZipState):
#     requested_ids: set[int]




# @dataclass(frozen=True)
# class DownstreamPending(ZipState, ABC):
#     ...

# @dataclass(frozen=True)
# class NonCompleted(ZipState):
#     completed_ids: set[int]

# @dataclass(frozen=True, slots=True)
# class Completed(NonActive):
#     """Summarizes states that represent a completed or errored Flowable"""
#
#
# @dataclass(frozen=True, slots=True)
# class CancelledUpstreamPending(NonActive): ...


# @dataclass(frozen=True, slots=True)
# class Terminated(ZipState):
#     """
#     Represents a successfully cancelled Flowable
#     """


# @dataclass(frozen=True)
# class AwaitUpstream(Active, NonCompleted):
#     """ Summarizes states that await further upstream notifications. """

    # @override
    # def remove(self, id: int, completed: bool):
    #     # Remove current upstream from the requested upstream IDs
    #     requested_ids = self.requested_ids - {id}
    #
    #     if completed:
    #         n_completed_ids = self.completed_ids | {id}
    #     else:
    #         n_completed_ids = self.completed_ids
    #
    #     if len(requested_ids):
    #
    #         return AwaitUpstream(
    #             requested_ids=requested_ids,
    #             completed_ids=n_completed_ids,
    #         )
    #
    #     # All requested items received
    #     else:
    #         return AwaitRequest(
    #             completed_ids=n_completed_ids,
    #         )


# @dataclass(frozen=True, slots=True)
# class AwaitRequest(DownstreamPending, NonCompleted):
#     """Send item downstream."""



    # complete_downstream: bool

    # complete: bool

    # @override
    # def remove(self, id: int, completed: bool):
    #     # Remove current upstream from the requested upstream IDs
    #     requested_ids = self.requested_ids - {id}
    #
    #     return Completed(
    #         requested_ids=requested_ids,
    #     )


# @dataclass(frozen=True, slots=True)
# class Complete(CompletedUpstreamPending):
#     """Error downstream observer."""


# @dataclass(frozen=True, slots=True)
# class Completed(ZipState):
#     complete: bool


    # requested_ids: dict[int, int]

    # def remove(self, id: int):
    #     if id not in self.requested_ids:
    #         return Error(f'{self.requested_ids} does not contain {id}')
    #
    #     # remove current upstream ID
    #     requested_ids = {id: idx for id, idx in self.requested_ids.items() if id != id}
    #
    #     return Cancel(
    #         requested_ids=requested_ids,
    #         index=requested_ids[id],
    #     )




# @dataclass(frozen=True, slots=True)
# class Terminate(Terminated):
#     """
#     Represents a successfully cancelled Flowable
#     """
#
#     index: int

# @dataclass(frozen=True, slots=True)
# class HasTerminatedState(Terminated):
#     """Has previously been terminated"""


# Old Implementations
#####################
#
# @dataclass(frozen=True)
# class ActiveStateOldMixin[U](ZipState):
#     """
#     Represents states where the Zip operator is active.
#
#     Depending on the provided selector, some upstream flowables are hold back (backpressured).
#     In this case, observers and values attribute are initially not empty.
#     """
#
#     # # Deferred rxsingle observers used to request a new item from inactive upstream flowables.
#     # observers: dict[int, DeferredHandler]
#
#     # # Received values
#     # values: dict[int, U]
#     received_id: set[int]
#
#
# # @dataclass(frozen=True)
# # class StopContinuationStateMixin:
# #     """Stop continuations associated with incoming upstream call"""
# #
# #     # Certificate is used to stop continuation of calling upstream flowable.
# #     certificate: ContCert
#
#
# @dataclass(frozen=True)
# class AwaitUpstreamStateOldMixin(ActiveStateMixin):
#     """Represents states where the Zip operator is awaiting upstream items."""
#
#     # Certificates returned when requesting new upstream item, one is returned to downstream flowable during subscription.
#     # Hence, the number qquals the number of active upstream flowables minus one.
#     certificates: tuple[ContCert, ...]
#
#     # Zip operator is scheduled to complete when all upstream items are received.
#     is_completed: bool
#
#
# @dataclass(frozen=True, slots=True)
# class AwaitOnNextState(AwaitUpstreamStateMixin):
#     """Request a new item from all non-backpressured upstream flowables"""
#
#
# @dataclass(frozen=True, slots=True)
# class AwaitFurtherState(StopContinuationStateMixin, AwaitUpstreamStateMixin):
#     """At least one upstream item received, await futher items."""
#
#
# @dataclass(frozen=True, slots=True)
# class OnNextState(ActiveStateMixin):
#     """Send item downstream."""
#
#
# @dataclass(frozen=True, slots=True)
# class TerminatedStateMixin(ZipState):
#     """Flowable either completed, errored, or cancelled"""
#
#     # Assign certificate to each active upstream flowable.
#     # This is important as the certificate is either used:
#     # - to stop calling upsream flowable (on_next, on_next_and_complete, ...), or
#     # - to cancel upstream flowable
#     certificates: dict[int, ContCert]
#
#
# @dataclass(frozen=True, slots=True)
# class OnNextAndCompleteState(ActiveStateMixin, TerminatedStateMixin):
#     """Send item and complete downstream observer."""
#
#
# @dataclass(frozen=True, slots=True)
# class OnCompletedState(TerminatedStateMixin):
#     """Complete downstream observer."""
#
#
# @dataclass(frozen=True, slots=True)
# class OnErrorState(TerminatedStateMixin):
#     """Error downstream observer."""
#
#     exception: Exception
#
#
# @dataclass(frozen=True, slots=True)
# class CancelledState(TerminatedStateMixin):
#     """Flowable is cancelled."""
#
#
# @dataclass(frozen=True, slots=True)
# class CancelledAwaitRequestState(ZipState):
#     """Flowable is cancelled, but downstream request pending."""
#
#     certificate: ContCert
#
#
# @dataclass(frozen=True, slots=True)
# class HasTerminatedState(StopContinuationStateMixin, TerminatedStateMixin):
#     """Has previously been terminated"""
