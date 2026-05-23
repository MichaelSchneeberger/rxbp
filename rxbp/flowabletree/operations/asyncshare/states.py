from __future__ import annotations

from abc import ABC
from dataclasses import dataclass, replace

from contcert.typing import ContCert


@dataclass(frozen=True)
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
        return replace(
            self,
            mapping=mapping if mapping is not None else self.mapping,
            first_index=first_index if first_index is not None else self.first_index,
            next_index=next_index if next_index is not None else self.next_index,
        )


class ShareState(ABC):
    pass


@dataclass(frozen=True)
class ErrorState(ShareState):
    msg: str


@dataclass(frozen=True)
class ActiveState(ShareState):
    counter: int

    # Maps each downstream to its current buffer item
    buffer_map: BufferMap

    is_completed: bool

    cancelled_certs: tuple[ContCert, ...]

    def copy(
        self, 
        counter: int | None = None,
        buffer_map: BufferMap | None = None,
        is_completed: bool | None = None,
    ):
        return replace(
            self,
            counter=counter if counter is not None else self.counter,
            buffer_map=buffer_map if buffer_map is not None else self.buffer_map,
            is_completed=is_completed if is_completed is not None else self.is_completed,
        )


@dataclass(frozen=True)
class CompletedState(ShareState):
    pass


@dataclass(frozen=True, slots=True)
class StopContinuationState(ShareState):
    index: int
