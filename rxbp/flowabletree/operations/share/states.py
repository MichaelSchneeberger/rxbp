from __future__ import annotations

from abc import ABC
from dataclasses import dataclass, replace

from contcert.typing import ContCert


@dataclass
class BufferMap:
    # Maps each downstream to its current buffer item
    mapping: dict[int, int]

    # Represents the item saved in the first position of the buffer
    first_index: int

    # Represents the next item that will be appended to the buffer
    next_index: int

    def copy(
        self, 
        mapping: dict[int, int] | None = None,
        first_index: int | None = None,
        next_index: int | None = None,
    ):
        def gen_args():
            if mapping is not None:
                yield 'mapping', mapping
            if first_index is not None:
                yield 'first_index', first_index
            if next_index is not None:
                yield 'next_index', next_index

        return replace(self, **dict(gen_args()))

    # @staticmethod
    # def update_first_index(mapping: dict[int, int], first_index: int):
    #     is_consumed = True
    #
    #     while is_consumed:
    #         # number of downstream observer that point to the first buffer position
    #         n_observers = sum(
    #             index
    #             for index in mapping.values()
    #             if index == first_index
    #         )
    #
    #         # the first position in the buffer is consumed by all downstream observers
    #         is_consumed = n_observers == 0
    #
    #         if not is_consumed:
    #             break
    #
    #         first_index += 1
    #
    #     return first_index



# enumeration of actions
@dataclass
class ActionEnum(ABC): ...


@dataclass
class PopFirst(ActionEnum): ...


@dataclass
class ShareState: ...


@dataclass
class Active(ShareState):
    # Maps each downstream to its current buffer item
    buffer_map: BufferMap

    # requested_ids: tuple[int, ...]
    is_completed: bool

    cancelled_certs: tuple[ContCert, ...]

    # action: ActionEnum

    def copy(
        self, 
        buffer_map: BufferMap | None = None,
        is_completed: bool | None = None,
        # action: ActionEnum | None = None,
    ):
        def gen_args():
            if is_completed is not None:
                yield 'is_completed', is_completed

            if buffer_map is not None:
                yield 'buffer_map', buffer_map

            # if action is not None:
            #     yield 'action', action
            # else:
            #     yield 'action', ActionEnum()

        return replace(self, **dict(gen_args()))


# todo: Holds no values -> remove?
@dataclass
class Completed(ShareState): ...
    # Flowable completed or errored, or all downstream Flowables cancelled

@dataclass
class Error(ShareState):
    msg: str


# class ShareState:
#     pass
#
#
# @dataclass(frozen=False)
# class ActiveStateMixin(ShareState):
#     """Further items may be pending to be sent."""
#
#     # Maps each downstream ID to its current (virtual) buffer index.
#     # This map determines to which downstream observers new items are sent to.
#     buffer_map: dict[int, int]
#
#     # The first buffer index is used to map the virtual buffer index to the actual buffer index.
#     # Explanation: Old and unused items are removed from the buffer without updating the indices
#     # in the buffer_map. Instead, only the first buffer index is updated, pointing to the first
#     # virtual index of the buffer.
#     first_buffer_index: int
#
#     # The last buffer index determines if a new item is taken from the buffer, or requested from upstream.
#     last_buffer_index: int
#
#     # Store downstream virtual_counts, as the downstream virtual_counts are dynamically adjusted.
#     virtual_counts: dict[int, int]
#
#     # Containers that hold certificates for each downstream observer that is either cancelled,
#     # or does not yet point to the last buffer item.
#     # These certificates are either consumed when downstream observers request a new item, or
#     # when receiving an upstream item.
#     cancelled_certificates: dict[int, ContCert]
#     requested_certificates: dict[int, ContCert]
#
#     is_completed: bool
#
#
# @dataclass(frozen=False)
# class StopContinuationStateMixin:
#     """Stop continuing exeuction"""
#
#     # Certificate is used to stop continuation of calling upstream flowable.
#     certificate: ContCert
#
#
# @dataclass(frozen=False)
# class RemoveBufferItemsStateMixin(ActiveStateMixin):
#     """Delete first n items from the buffer"""
#
#     n_buffer_pop: int
#
#
# @dataclass(frozen=False)
# class AwaitUpstreamStateMixin(ActiveStateMixin):
#     """At least one downstream observer requested a new item"""
#
#
# @dataclass(frozen=False)
# class InitState(AwaitUpstreamStateMixin):
#     pass
#
#
# @dataclass(frozen=False, slots=True)
# class RequestUpstream(AwaitUpstreamStateMixin):
#     """Request new upstream item on first downstream request"""
#
#     pass
#
#
# @dataclass(frozen=False, slots=True)
# class AwaitOnNext(AwaitUpstreamStateMixin):
#     """New upstream item has already been requested, await on_next call"""
#
#     certificate: ContCert
#
#
# @dataclass(frozen=False, slots=True)
# class OnNextFromBuffer(AwaitUpstreamStateMixin):
#     """Send item from buffer."""
#
#     index: int
#     pop_item: bool
#
#
# @dataclass(frozen=False)
# class AwaitUpstreamAfterCancelState(
#     RemoveBufferItemsStateMixin, AwaitUpstreamStateMixin
# ):
#     """Downstream branch has been cancelled, remove items from buffer if necessary."""
#
#     pass
#
#
# @dataclass(frozen=False)
# class AwaitDownstreamStateMixin(ActiveStateMixin):
#     """Newest upstream item has benn sent downstream, await downstream request."""
#
#     pass
#
#
# @dataclass(frozen=False)
# class AwaitDownstreamAfterCancelState(
#     RemoveBufferItemsStateMixin, AwaitDownstreamStateMixin
# ):
#     """Downstream branch has been cancelled, remove items from buffer if necessary."""
#
#     pass
#
#
# @dataclass(frozen=False)
# class RequestFromCancelledDownstreamState(
#     StopContinuationStateMixin, AwaitDownstreamStateMixin
# ):
#     """New item requested after downstream was cancelled, stop continuation using the certificate."""
#
#     pass
#
#
# @dataclass(frozen=False, slots=True)
# class OnNext(AwaitDownstreamStateMixin):
#     """New item received."""
#
#     # IDs referencing downstream observers that poin to the last item in the buffer
#     send_ids: tuple[int, ...]
#
#     buffer_item: bool
#
#
# @dataclass(frozen=True, slots=True)
# class TerminatedStateMixin(ShareState):
#     requested_certificates: dict[int, ContCert]
#
#
# @dataclass(frozen=True, slots=True)
# class OnCompletedState(TerminatedStateMixin):
#     send_ids: tuple[int, ...]
#
#
# @dataclass(frozen=True, slots=True)
# class ErrorStateMixin(TerminatedStateMixin):
#     exception: Exception
#
#
# @dataclass(frozen=True, slots=True)
# class OnErrorState(ErrorStateMixin):
#     send_ids: tuple[int, ...]
#
#
# @dataclass(frozen=True, slots=True)
# class HasErroredState(ErrorStateMixin):
#     pass
#
#
# @dataclass(frozen=True, slots=True)
# class CancelledState(TerminatedStateMixin):
#     pass
#
#
# @dataclass(frozen=True, slots=True)
# class HasTerminatedState(TerminatedStateMixin):
#     pass
