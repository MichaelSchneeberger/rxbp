from __future__ import annotations

from dataclasses import dataclass

from continuationmonad.typing import ContinuationCertificate

from rxbp.cancellable import Cancellable


class FlatMapState:
    pass


@dataclass(frozen=False)
class ActiveState(FlatMapState):
    cancellable: Cancellable


@dataclass(frozen=True)
class CancelledState(FlatMapState):
    cancellable: Cancellable
    certificate: ContinuationCertificate
