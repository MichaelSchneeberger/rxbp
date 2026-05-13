from __future__ import annotations

from abc import ABC
from dataclasses import dataclass


class MergeState:
    pass


@dataclass(frozen=True)
class ErrorState(MergeState):
    msg: str


@dataclass(frozen=True)
class ActiveState(MergeState, ABC):
    active_ids: set[int]
    received_ids: tuple[int, ...]


@dataclass(frozen=True)
class RequestState(ActiveState, ABC):
    id: int


@dataclass(frozen=True)
class CompletedState(MergeState, ABC):
    assigned_ids: dict[int, int]
    received_ids: tuple[int, ...]


@dataclass(frozen=True, slots=True)
class StopContinuationState(CompletedState):
    index: int

