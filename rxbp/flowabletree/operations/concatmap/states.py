from __future__ import annotations

from dataclasses import dataclass

from contcert.typing import ContCert

from rxbp.cancellable import Cancellable


class ConcatMapState:
    pass


@dataclass(frozen=False)
class ActiveState(ConcatMapState):
    cancellable: Cancellable


@dataclass(frozen=True)
class CancelledState(ConcatMapState):
    cancellable: Cancellable
    certificate: ContCert
