from __future__ import annotations

from abc import ABC
from dataclasses import dataclass


class ZipState:
    pass


@dataclass(frozen=True)
class ErrorState(ZipState):
    msg: str


@dataclass(frozen=True)
class ActiveState(ZipState, ABC):
    requested_ids: set[int]
    completed_ids: set[int]


@dataclass(frozen=True)
class CompletedState(ZipState, ABC):
    assigned_ids: dict[int, int]


@dataclass(frozen=True, slots=True)
class StopContinuationState(CompletedState):
    index: int
