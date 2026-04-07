from __future__ import annotations

from abc import ABC
from dataclasses import dataclass


class ZipState: ...


@dataclass(frozen=True)
class Error(ZipState):
    msg: str


@dataclass(frozen=True)
class Active(ZipState, ABC):
    requested_ids: set[int]
    completed_ids: set[int]


@dataclass(frozen=True)
class Completed(ZipState, ABC):
    assigned_ids: dict[int, int]


@dataclass(frozen=True, slots=True)
class StopContinuation(Completed):
    index: int
