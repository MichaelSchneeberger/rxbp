
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import override

from rxbp.flowabletree.operations.asyncshare.states import (
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
                mapping={},
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
                return p_state

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
                return p_state.copy(
                    # buffer_map=n_buffer_map,
                    is_completed=True,
                )

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

            case _:
                return Error(f"Unknown State {p_state}")

