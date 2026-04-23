
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

# enumeration of actions
@dataclass
class ActionEnum(ABC): ...


@dataclass
class PopFirst(ActionEnum): ...


@dataclass
class ShareState: ...


# class Subscribe(ShareState):
#     pass



@dataclass
class Active(ShareState):
    # Maps each downstream to its current buffer item
    buffer_map: BufferMap

    # requested_ids: tuple[int, ...]
    is_completed: bool

    cancelled_certs: tuple[ContCert, ...]

    def copy(
        self, 
        buffer_map: BufferMap | None = None,
        is_completed: bool | None = None,
    ):
        def gen_args():
            if is_completed is not None:
                yield 'is_completed', is_completed

            if buffer_map is not None:
                yield 'buffer_map', buffer_map

        return replace(self, **dict(gen_args()))


# todo: Holds no values -> remove?
@dataclass
class Completed(ShareState): ...
    # Flowable completed or errored, or all downstream Flowables cancelled

@dataclass
class Error(ShareState):
    msg: str

