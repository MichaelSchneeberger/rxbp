from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import override

from rxbp.flowabletree.operations.share.states import (
    ActiveState,
    BufferMap,
    ShareState,
    ErrorState,
)


class ShareStateTransition(ABC):
    @abstractmethod
    def get_state(self) -> ShareState: ...


@dataclass(frozen=True)
class ToStateTransition(ShareStateTransition):
    state: ShareState

    @override
    def get_state(self) -> ShareState:
        return self.state


@dataclass
class InitialTransition(ShareStateTransition):
    @override
    def get_state(self) -> ActiveState:
        return ActiveState(
            buffer_map=BufferMap(
                mapping={0: 0},
                first_index=0,
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
    def get_state(self) -> ActiveState | ErrorState:
        match p_state := self.child.get_state():
            case ActiveState(
                buffer_map=buffer_map,
            ):
                if self.id in buffer_map.mapping:
                    return ErrorState("ID already in mapping")

                n_mapping = dict(buffer_map.mapping) | {self.id: 0}

                n_buffer_map = buffer_map.copy(mapping=n_mapping)

                return p_state.copy(buffer_map=n_buffer_map)

            case _:
                return ErrorState(f"Unknown State {p_state}")


@dataclass
class OnNextTransition(ShareStateTransition):
    child: ShareStateTransition

    @override
    def get_state(self) -> ActiveState | ErrorState:
        match p_state := self.child.get_state():
            case ActiveState(
                buffer_map=buffer_map,
            ):
                return p_state.copy(
                    buffer_map=buffer_map.copy(
                        next_index=buffer_map.next_index + 1,
                    ),
                )

            case _:
                return ErrorState(f"Unknown State {p_state}")


@dataclass
class OnNextAndCompleteTransition(ShareStateTransition):
    child: ShareStateTransition

    @override
    def get_state(self) -> ActiveState | ErrorState:
        match p_state := self.child.get_state():
            case ActiveState():
                return p_state.copy(
                    is_completed=True,
                )

            case _:
                return ErrorState(f"Unknown State {p_state}")


@dataclass
class OnCompletedTransition(ShareStateTransition):
    child: ShareStateTransition

    @override
    def get_state(self) -> ActiveState | ErrorState:
        match p_state := self.child.get_state():
            case ActiveState():
                return p_state.copy(is_completed=True)

            case _:
                return ErrorState(f"Unknown State {p_state}")


@dataclass
class RequestTransition(ShareStateTransition):
    child: ShareStateTransition
    id: int

    @override
    def get_state(self) -> ActiveState | ErrorState:
        match p_state := self.child.get_state():
            case ActiveState(buffer_map=buffer_map):
                n_mapping = buffer_map.mapping.copy()
                index = n_mapping[self.id]
                n_index = index + 1

                # increase index in buffer mapping
                n_mapping[self.id] = n_index

                # update first index if necessary
                if index == buffer_map.first_index:
                    n_observers = sum(
                        1
                        for idx in n_mapping.values()
                        if idx == buffer_map.first_index
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
                        next_index=buffer_map.next_index,
                    ),
                )

            case _:
                return ErrorState(f"Unknown State {p_state}")
