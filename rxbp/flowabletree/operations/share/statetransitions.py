from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import override

from rxbp.flowabletree.operations.share.states import (
    Active,
    BufferMap,
    ShareState,
    Error,
)


@dataclass
class ShareStateTransition(ABC):
    @abstractmethod
    def get_state(self) -> ShareState: ...


@dataclass
class ToStateTransition(ShareStateTransition):
    state: ShareState

    @override
    def get_state(self) -> ShareState:
        return self.state


@dataclass
class InitialTransition(ShareStateTransition):
    @override
    def get_state(self) -> Active:
        return Active(
            buffer_map=BufferMap(
                mapping={0: 0},
                first_index=-1,
                next_index=0,
            ),
            is_completed=False,
            cancelled_certs=tuple(),
        )


@dataclass
class SubscribeTransition(ShareStateTransition):
    child: ShareStateTransition
    id: int

    @override
    def get_state(self) -> Active | Error:
        match p_state := self.child.get_state():
            case Active(
                buffer_map=buffer_map,
            ):
                if self.id in buffer_map.mapping:
                    return Error("Not good")

                n_mapping = dict(buffer_map.mapping) | {self.id: 0}

                n_buffer_map = buffer_map.copy(mapping=n_mapping)

                return p_state.copy(buffer_map=n_buffer_map)

            case _:
                return Error(f"Unknown State {p_state}")


@dataclass
class OnNextTransition(ShareStateTransition):
    child: ShareStateTransition

    @override
    def get_state(self) -> Active | Error:
        match p_state := self.child.get_state():
            case Active(
                buffer_map=buffer_map,
            ):
                # n_buffer_map = buffer_map.copy(
                #     next_index=buffer_map.next_index + 1,
                # )

                # return p_state.copy(
                #     buffer_map=n_buffer_map,
                # )
                return p_state

            # case Completed():
            #     return p_state

            case _:
                return Error(f"Unknown State {p_state}")


@dataclass
class OnNextAndCompleteTransition(ShareStateTransition):
    child: ShareStateTransition

    @override
    def get_state(self) -> Active | Error:
        match p_state := self.child.get_state():
            case Active(
                buffer_map=buffer_map,
            ):
                # n_buffer_map = buffer_map.copy(
                #     next_index=buffer_map.next_index + 1,
                # )

                return p_state.copy(
                    # buffer_map=n_buffer_map,
                    is_completed=True,
                )

            # case Completed():
            #     return p_state

            case _:
                return Error(f"Unknown State {p_state}")


@dataclass
class OnCompletedTransition(ShareStateTransition):
    child: ShareStateTransition

    @override
    def get_state(self) -> Active | Error:
        match p_state := self.child.get_state():
            case Active():
                return p_state.copy(is_completed=True)

            # case Completed():
            #     return p_state

            case _:
                return Error(f"Unknown State {p_state}")


@dataclass
class RequestTransition(ShareStateTransition):
    child: ShareStateTransition
    id: int

    @override
    def get_state(self) -> Active | Error:
        match p_state := self.child.get_state():
            case Active(buffer_map=buffer_map):
                # increase index in buffer mapping
                n_mapping = buffer_map.mapping.copy()
                index = n_mapping[self.id]
                n_index = index + 1
                n_mapping[self.id] = n_index

                # update first index if necessary
                if index == buffer_map.first_index:
                    n_observers = sum(
                        index
                        for index in n_mapping.values()
                        if index == buffer_map.first_index
                    )

                    if n_observers == 0:
                        n_first_index = n_index
                    else:
                        n_first_index = index

                else:
                    n_first_index = buffer_map.first_index

                return p_state.copy(
                    buffer_map=buffer_map.copy(
                        mapping=n_mapping,
                        first_index=n_first_index,
                        next_index=buffer_map.next_index + 1,
                    ),
                    # action=action,
                )

            # case Completed():
            #     return p_state

            case _:
                return Error(f"Unknown State {p_state}")


#             case AwaitUpstreamStateMixin():
#                 last_buffer_index = state.last_buffer_index + 1
#
#                 send_ids = tuple(
#                     id
#                     for id, index in state.buffer_map.items()
#                     if index == last_buffer_index
#                 )
#
#                 # not all subscribers request new item
#                 buffer_item = state.first_buffer_index != last_buffer_index
#
#                 return OnNext(
#                     buffer_map=state.buffer_map,
#                     first_buffer_index=state.first_buffer_index,
#                     last_buffer_index=last_buffer_index,
#                     virtual_counts=state.virtual_counts,
#                     requested_certificates=state.requested_certificates,
#                     send_ids=send_ids,
#                     buffer_item=buffer_item,
#                     is_completed=False,
#                     cancelled_certificates=state.cancelled_certificates,
#                 )
#
#             case CancelledState(requested_certificates=requested_certificates):
#                 return HasTerminatedState(requested_certificates=requested_certificates)
#
#             case _:
#                 raise Exception(
#                     f"Unexpected state {state} returned by action {self.child}."
#                 )
#
#
# @dataclass
# class RequestTransition(ShareStateTransition):
#     """Downstream request received"""
#
#     child: ShareStateTransition
#     id: int  # downstream id
#     virtual_count: int  # downstream virtual_count
#
#     def get_state(self):
#         match state := self.child.get_state():
#             case ActiveStateMixin():
#
#                 def gen_virtual_counts():
#                     for id, w in state.virtual_counts.items():
#                         if id == self.id:
#                             yield id, self.virtual_count
#                         else:
#                             yield id, w
#
#                 virtual_counts = dict(gen_virtual_counts())
#
#                 if self.id in state.cancelled_certificates:
#                     return RequestFromCancelledDownstreamState(
#                         buffer_map=state.buffer_map,
#                         first_buffer_index=state.first_buffer_index,
#                         last_buffer_index=state.last_buffer_index,
#                         virtual_counts=virtual_counts,
#                         requested_certificates=state.requested_certificates,
#                         certificate=state.cancelled_certificates[self.id],
#                         is_completed=state.is_completed,
#                         cancelled_certificates=state.cancelled_certificates,
#                     )
#
#                 # update buffer_map
#                 buffer_map = {}
#                 sel_index = None
#                 for id, index in state.buffer_map.items():
#                     if id == self.id:
#                         sel_index = index
#                         buffer_map[id] = index + 1
#
#                     else:
#                         buffer_map[id] = index
#
#                 assert sel_index is not None, "id not found in buffer."
#
#                 # there is no item in the buffer
#                 if state.last_buffer_index == sel_index:
#                     # request new upstream item
#                     if isinstance(state, AwaitDownstreamStateMixin):
#                         return RequestUpstream(
#                             buffer_map=buffer_map,
#                             first_buffer_index=state.first_buffer_index,
#                             last_buffer_index=state.last_buffer_index,
#                             virtual_counts=virtual_counts,
#                             requested_certificates=state.requested_certificates,
#                             # certificate=None,
#                             is_completed=state.is_completed,
#                             cancelled_certificates=state.cancelled_certificates,
#                         )
#
#                     else:
#                         certificate = state.requested_certificates[self.id]
#                         # requested_certificates = {id: c for id, c in requested_certificates.items() if id != self.id}
#
#                         return AwaitOnNext(
#                             buffer_map=buffer_map,
#                             first_buffer_index=state.first_buffer_index,
#                             last_buffer_index=state.last_buffer_index,
#                             virtual_counts=virtual_counts,
#                             requested_certificates=state.requested_certificates,
#                             certificate=certificate,
#                             is_completed=state.is_completed,
#                             cancelled_certificates=state.cancelled_certificates,
#                         )
#
#                 # there is an item in the buffer
#                 else:
#                     first_buffer_index = state.first_buffer_index
#
#                     # update first index
#                     if sel_index == first_buffer_index:
#                         # number of downstream observers pointing to the first buffer item
#                         n_observers = sum(
#                             index
#                             for index in buffer_map.values()
#                             if index == state.first_buffer_index
#                         )
#
#                         # no upstream observers point to the first buffer item, increase index
#                         if pop_item := n_observers == 0:
#                             first_buffer_index = first_buffer_index + 1
#
#                     else:
#                         pop_item = False
#
#                     return OnNextFromBuffer(
#                         buffer_map=buffer_map,
#                         first_buffer_index=first_buffer_index,
#                         last_buffer_index=state.last_buffer_index,
#                         virtual_counts=virtual_counts,
#                         requested_certificates=state.requested_certificates,
#                         is_completed=state.is_completed,
#                         index=sel_index,
#                         pop_item=pop_item,
#                         cancelled_certificates=state.cancelled_certificates,
#                     )
#
#             case ErrorStateMixin(
#                 exception=exception,
#                 requested_certificates=requested_certificates,
#             ):
#                 return HasErroredState(
#                     exception=exception,
#                     requested_certificates=requested_certificates,
#                 )
#
#             case TerminatedStateMixin(requested_certificates=requested_certificates):
#                 return HasTerminatedState(requested_certificates=requested_certificates)
#
#             case _:
#                 raise Exception(f"Unexpected state {state}.")
#
#
# @dataclass(frozen=False)
# class OnNextAndCompleteTransition(ShareStateTransition):
#     """Item received, stream is complete"""
#
#     child: ShareStateTransition
#
#     def get_state(self):
#         match state := self.child.get_state():
#             case AwaitUpstreamStateMixin(
#                 buffer_map=buffer_map,
#                 first_buffer_index=first_buffer_index,
#                 last_buffer_index=last_buffer_index,
#                 # is_ack=is_ack,
#                 requested_certificates=requested_certificates,
#             ):
#                 n_last_buffer_index = last_buffer_index + 1
#
#                 send_ids = tuple(
#                     id
#                     for id, index in buffer_map.items()
#                     if index == n_last_buffer_index
#                 )
#
#                 buffer_item = (
#                     first_buffer_index != n_last_buffer_index
#                 )  # all subscribers request new item
#
#                 return OnNext(
#                     buffer_map=buffer_map,
#                     first_buffer_index=first_buffer_index,
#                     last_buffer_index=n_last_buffer_index,
#                     virtual_counts=state.virtual_counts,
#                     requested_certificates=requested_certificates,
#                     send_ids=send_ids,
#                     buffer_item=buffer_item,
#                     is_completed=True,
#                     cancelled_certificates=state.cancelled_certificates,
#                 )
#
#             case CancelledState(requested_certificates=requested_certificates):
#                 return HasTerminatedState(requested_certificates=requested_certificates)
#
#             case _:
#                 raise Exception(f"Unexpected state {state}.")
#
#
# @dataclass(frozen=False)
# class OnCompletedTransition(ShareStateTransition):
#     """Stream is complete"""
#
#     child: ShareStateTransition
#
#     def get_state(self):
#         match state := self.child.get_state():
#             case ActiveStateMixin(
#                 buffer_map=buffer_map,
#                 last_buffer_index=last_buffer_index,
#                 requested_certificates=requested_certificates,
#             ):
#                 send_ids = tuple(
#                     id for id, index in buffer_map.items() if last_buffer_index <= index
#                 )
#
#                 return OnCompletedState(
#                     send_ids=send_ids,
#                     requested_certificates=requested_certificates,
#                 )
#
#             case CancelledState(requested_certificates=requested_certificates):
#                 return HasTerminatedState(requested_certificates=requested_certificates)
#
#             case _:
#                 raise Exception(f"Unexpected state {state}.")
#
#
# @dataclass(frozen=False)
# class OnErrorTransition(ShareStateTransition):
#     child: ShareStateTransition
#     exception: Exception
#
#     def get_state(self):
#         match state := self.child.get_state():
#             case ActiveStateMixin(
#                 buffer_map=buffer_map,
#                 last_buffer_index=last_buffer_index,
#                 requested_certificates=requested_certificates,
#             ):
#                 send_ids = tuple(
#                     id for id, index in buffer_map.items() if index == last_buffer_index
#                 )
#
#                 return OnErrorState(
#                     exception=self.exception,
#                     send_ids=send_ids,
#                     requested_certificates=requested_certificates,
#                 )
#
#             case CancelledState(requested_certificates=requested_certificates):
#                 return HasTerminatedState(requested_certificates=requested_certificates)
#
#             case _:
#                 raise Exception(f"Unexpected state {state}.")
#
#
# @dataclass(frozen=False)
# class CancelTransition(ShareStateTransition):
#     child: ShareStateTransition
#     id: int
#     certificate: ContCert
#
#     def get_state(self):
#         match state := self.child.get_state():
#             case ActiveStateMixin(
#                 buffer_map=buffer_map,
#                 requested_certificates=requested_certificates,
#                 first_buffer_index=first_buffer_index,
#             ):
#                 # remove downstrem from buffer mapping
#                 n_buffer_map = {
#                     id: index for id, index in buffer_map.items() if id != self.id
#                 }
#
#                 # Downstream certificate is concurrently validated in case the downstream cancellation
#                 # (i.e., downstream calls the cancel() method) occurs after requesting a new item.
#                 # In this case, we select the valid certificate using or_ operator.
#                 certificate = requested_certificates[self.id].or_(self.certificate)
#
#                 if len(n_buffer_map) == 0:
#                     # no active downstream observers
#
#                     requested_certificates = requested_certificates | {
#                         self.id: certificate
#                     }
#
#                     return CancelledState(
#                         requested_certificates=requested_certificates,
#                     )
#
#                 else:
#                     first_buffer_index = min(n_buffer_map.values())
#                     n_buffer_pop = first_buffer_index - first_buffer_index
#
#                     match state:
#                         case AwaitUpstreamStateMixin():
#                             requested_certificates = requested_certificates | {
#                                 # self.id: certificate
#                                 self.id: self.certificate  # this should also work
#                             }
#
#                             return AwaitUpstreamAfterCancelState(
#                                 buffer_map=n_buffer_map,
#                                 first_buffer_index=first_buffer_index,
#                                 last_buffer_index=state.last_buffer_index,
#                                 virtual_counts=state.virtual_counts,
#                                 requested_certificates=requested_certificates,
#                                 is_completed=state.is_completed,
#                                 n_buffer_pop=n_buffer_pop,
#                                 cancelled_certificates=state.cancelled_certificates,
#                             )
#
#                         case AwaitDownstreamStateMixin(
#                             cancelled_certificates=cancelled_certificates,
#                         ):
#                             # certificate is required to stop the continuation when the downstream
#                             # is requesting a new item (despite being cancelled)
#                             cancelled_certificates = cancelled_certificates | {
#                                 self.id: certificate
#                             }
#
#                             requested_certificates = {
#                                 id: c
#                                 for id, c in requested_certificates.items()
#                                 if id != self.id
#                             }
#
#                             return AwaitDownstreamAfterCancelState(
#                                 buffer_map=n_buffer_map,
#                                 first_buffer_index=first_buffer_index,
#                                 last_buffer_index=state.last_buffer_index,
#                                 virtual_counts=state.virtual_counts,
#                                 requested_certificates=requested_certificates,
#                                 is_completed=state.is_completed,
#                                 n_buffer_pop=n_buffer_pop,
#                                 cancelled_certificates=cancelled_certificates,
#                             )
#
#             case TerminatedStateMixin(requested_certificates=requested_certificates):
#                 return HasTerminatedState(requested_certificates=requested_certificates)
#
#             case _:
#                 raise Exception(f"Unexpected state {state}.")
