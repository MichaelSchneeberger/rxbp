from __future__ import annotations

import functools
from enum import Enum, auto

from dataclasses import dataclass, replace

from continuationmonad.typing import ContinuationCertificate
from continuationmonad.typing import DeferredHandler, ContinuationCertificate

from rxbp.cancellable import Cancellable

"""
States
------

State groups (-), states (>):
- Active - AwaitUpstream
  > Init
  > RequestUpstream
  > AwaitOnNext
  > OnNextFromBuffer
  > AwaitUpstreamAfterCancel
- Active - AwaitDownstream
  > OnNext
  > AwaitDownstreamAfterCancel
- Terminated
  > OnCompleted
  > Cancelled
  > HasTerminated
- Terminated - Errored
  > OnError
  > HasErrored
"""

@dataclass(frozen=False)
class StopContinuationStateMixin:
    """Stop continuing exeuction"""

    # Certificate is used to stop continuation of calling upstream flowable.
    certificate: ContinuationCertificate



class ShareState:
    pass


# Downstream ID
type DownstreamID = int

# Received item index
type ItemIndex = int


@dataclass(frozen=False)
class ActiveState(ShareState):
    """
    In active state, the async share observer has not terminated (either via on_completed or on_error).
    """

    @dataclass(frozen=True)
    class BufferMap:
        """
        Maintains a mapping between the received item index and the coresponding position in the internal buffer.
        """

        # Maps each downstream ID to its current item index.
        mapping: dict[DownstreamID, ItemIndex]

        # Represents the item saved in the first position of the buffer
        first_index: ItemIndex

        # Represents the next item that will be appended to the buffer
        # The last_buffer_index determines if either the next downstream item is taken from the buffer or requested from upst pvream.
        # It is updated when receiving on_next call.
        next_index: ItemIndex

        def get_active_downstream(self):
            yield self.mapping.keys()

        def is_waiting(self, id: DownstreamID):
            """
            The mapping points to the next item within the buffer that is sent downstream.

            If all items from the buffer have been sent to a downstream observer, it is considered ready to directly receive the next
            upstream item.
            """

            return self.mapping[id] < self.next_index

        def get_waiting_downstreams(self):
            """
            This function returns the IDs of all downstream observers that wait for the next upstream item.
            """

            for id, index in self.mapping.items():
                if index == self.next_index:
                    yield id

        def is_any_downstream_waiting(self):
            def inner():
                for index in self.mapping.values():
                    if index == self.next_index:
                        yield True

                yield False

            return next(inner())

        def increment(self, id: DownstreamID):
            """
            The buffer map is incremented when a downstream observer requests a new item.

            Returns:
              pop_buffer: Bool indicating if the first position of the buffer must be popped.
              buffer_map: Updated buffer mapping
            """

            # copy map and increase the corresponding item index
            mapping = self.mapping.copy()
            mapping[id] += 1

            first_index = self.update_first_index(mapping)

            n_buffer_pop = first_index - self.first_index

            assert n_buffer_pop <= 1

            # true if no more downstream observers point to the last position in the buffer_map
            pop_buffer = n_buffer_pop == 1

            buffer_map = self.copy(
                mapping=mapping,
                first_index=first_index,
            )

            return pop_buffer, buffer_map

        def update_first_index(self, mapping: dict[DownstreamID, ItemIndex]):
            """
            When the buffer map is modified, the first_index possibly needs to be updated as well.
            """

            is_consumed = True
            first_index = self.first_index

            while is_consumed:
                # number of downstream observer that point to the first buffer position
                n_observers = sum(
                    index
                    for index in mapping.values()
                    if index == self.first_index
                )

                # the first position in the buffer is consumed by all downstream observers
                is_consumed = n_observers == 0

                if not is_consumed:
                    break

                first_index += 1

            return first_index

        def downstream_cancels(self, id: DownstreamID):
            """
            An entry in the buffer map is removed when the corresponding downstream calls cancel.
            """

            mapping = self.mapping.copy()
            del mapping[id]

            first_index = self.update_first_index(mapping)

            n_buffer_pop = first_index - self.first_index

            buffer_map = self.copy(
                mapping=mapping,
                first_index=first_index,
            )

            return n_buffer_pop, buffer_map

        def copy(
            self, 
            mapping: dict[DownstreamID, ItemIndex] | None = None,
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

    # enumeration of actions
    class ActionEnum: ...

    class Subscribe(ActionEnum): ...

    class TrampolineSubscription(ActionEnum):
        """ Schedule trampoline before sending items downstream """
        ...

    class ActivateSubscription(ActionEnum): ...

    @dataclass(frozen=True)
    class OnNextBase(ActionEnum):
        buffer_map: ActiveState.BufferMap

        def should_add_to_buffer(self):
            buffer_map = self.buffer_map
            return buffer_map.first_index != buffer_map.next_index

    @dataclass(frozen=True)
    class OnNext(OnNextBase): ...

    @dataclass(frozen=True)
    class OnNextAndComplete(OnNextBase): ...

    @dataclass(frozen=True)
    class OnCompleted(ActionEnum): ...

    @dataclass(frozen=True)
    class OnError(ActionEnum): ...

    @dataclass(frozen=True)
    class RequestNextItem(ActionEnum):
        pop_buffer: bool

    @dataclass(frozen=True)
    class AwaitUpstream(ActionEnum):
        certificate: ContinuationCertificate

    class OnNextFromBuffer(ActionEnum):
        pass


    # Fields
    ########

    # counts downstream subscriptions
    subscription_cnt: int

    buffer_map: BufferMap

    # is_awaiting_upstream: bool

    is_completed: bool

    is_outer_cancelled: bool

    action: ActionEnum

    # # The cancellation returned when subscribing to the upstream observer.
    # upstream_cancellation: Cancellable | None
    #
    # # modified synchronously
    # deferred_handler: DeferredHandler | None

    # # Stores downstream weights, determined the weight requested from upstream
    # # The weights are updated when downstream requests the next item, weights are removed when downstream cancels.
    # weights: dict[int, int]

    # Stores certificates from requesting next item from upstream observer.
    _downstream_certificates: dict[int, ContinuationCertificate]
    _upstream_certificate: ContinuationCertificate


    def copy(
        self, 
        subscription_cnt: int | None = None,
        buffer_map: BufferMap | None = None,
        is_awaiting_upstream: bool | None = None,
        is_completed: bool | None = None,
        is_outer_cancelled: bool | None = None,
        action: ActionEnum | None = None,
    ):
        def gen_args():
            if subscription_cnt is not None:
                yield 'subscription_cnt', subscription_cnt
            if action is not None:
                yield 'action', action
            else:
                yield 'action', ActiveState.ActionEnum()
            if is_awaiting_upstream is not None:
                yield 'is_awaiting_upstream', is_awaiting_upstream
            if is_completed is not None:
                yield 'is_completed', is_completed
            if is_outer_cancelled is not None:
                yield 'is_outer_cancelled', is_outer_cancelled
            if buffer_map is not None:
                yield 'buffer_map', buffer_map

        return replace(self, **dict(gen_args()))

    # def get_subscription_count(self) -> int:
    #     return len(self._downstream_certificates)

    def get_downstream_certificate(self, id: DownstreamID):
        assert self._upstream_certificate.validated
        return self._downstream_certificates[id]



@dataclass(frozen=True, slots=True)
class TerminatedState(ShareState):
    pass



# @dataclass(frozen=False)
# class RemoveBufferItemsStateMixin(ActiveState):
#     """Delete first n items from the buffer"""
#
#     # n_buffer_pop: int


# Pre Active States
# ---------------------------


# @dataclass(frozen=False)
# class InitState(PreActiveStateMixin):
#     # certificates: tuple[ContinuationCertificate]
#     # upstream_cancellation: Cancellable
#     pass
#
#
# @dataclass(frozen=False)
# class SubscribeState(PreActiveStateMixin):
#     pass
#

# @dataclass(frozen=False)
# class SubscribeState(PreActiveStateMixin):
#     pass


# Await for upstream observer
# ---------------------------


# @dataclass(frozen=False)
# class AwaitUpstreamStateMixin(ActiveStateMixin):
#     """At least one downstream observer requested a new item"""
#
#     # requested_weights: dict[int, int]
#
#     # # Stores certificates from requesting next item from upstream observer.
#     # requested_certificates: dict[int, ContinuationCertificate]
#
#     def is_awaiting(self, id: int):
#         return self.last_buffer_index + 1 == self.buffer_map[id]
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
#     certificate: ContinuationCertificate
#
#
# @dataclass(frozen=False, slots=True)
# class OnNextFromBuffer(AwaitUpstreamStateMixin):
#     """Send item from buffer."""
#
#     index: int
#
#     # remove first item from buffer if the current downstream observer exclusively points
#     # (i.e., no one else)  the first item in the buffer.
#     pop_item: bool
#
#
# @dataclass(frozen=False)
# class AwaitUpstreamAfterCancelState(
#     RemoveBufferItemsStateMixin, AwaitUpstreamStateMixin
# ):
#     """Downstream branch has been cancelled, remove items from buffer if necessary."""
#
#
# @dataclass(frozen=False)
# class AwaitUpstreamAfterSubscriptionState(AwaitUpstreamStateMixin):
#     """Downstream branch has been cancelled, remove items from buffer if necessary."""
#
#     # certificate: ContinuationCertificate


# Await for upstream observer
# ---------------------------


@dataclass(frozen=False)
class AwaitDownstreamStateMixin(ActiveState):
    """
    on_next upstream call received, await first synced downstream request.
    There are no valid certificates stored in the shared observer.
    """

    # """Newest upstream item has been sent downstream, await downstream request."""

    def is_synced(self, id: int):
        return self.last_buffer_index == self.buffer_map[id]

    @property
    def buffer_item(self):
        return self.first_buffer_index != self.last_buffer_index


@dataclass(frozen=False)
class RequestFromCancelledDownstreamState(
    StopContinuationStateMixin, AwaitDownstreamStateMixin
):
    """New item requested after downstream was cancelled, stop continuation using the certificate."""


@dataclass(frozen=False, slots=True)
class OnNext(AwaitDownstreamStateMixin):
    """New item received."""

    # IDs referencing downstream observers that poin to the last item in the buffer
    # send_ids: tuple[int, ...]

    # buffer_item: bool


@dataclass(frozen=False)
class AwaitDownstreamAfterCancelState(
    RemoveBufferItemsStateMixin, AwaitDownstreamStateMixin
):
    """Downstream branch has been cancelled, remove items from buffer if necessary."""


@dataclass(frozen=False)
class AwaitDownstreamAfterSubscriptionState(AwaitDownstreamStateMixin):
    """Downstream branch has been cancelled, remove items from buffer if necessary."""

    # certificate: ContinuationCertificate


# Terminated
# ----------




@dataclass(frozen=True, slots=True)
class OnCompletedState(TerminatedState):
    send_ids: tuple[int, ...]
    requested_certificates: dict[int, ContinuationCertificate]
    # pass


# @dataclass(frozen=True, slots=True)
# class ErrorStateMixin(TerminatedStateMixin):
#     exception: Exception


@dataclass(frozen=True, slots=True)
class OnErrorState(TerminatedState):
    send_ids: tuple[int, ...]
    requested_certificates: dict[int, ContinuationCertificate]
    # pass


# @dataclass(frozen=True, slots=True)
# class HasErroredState(ErrorStateMixin):
#     pass


@dataclass(frozen=True, slots=True)
class CancelledState(TerminatedState):
    # requested_certificates: dict[int, ContinuationCertificate]
    certificate: ContinuationCertificate


# @dataclass(frozen=True, slots=True)
# class HasTerminatedState(TerminatedStateMixin):
#     pass
