from __future__ import annotations

from dataclasses import dataclass
from continuationmonad.typing import (
    ContinuationCertificate,
)

from rxbp.cancellable import Cancellable, CancellationState
from rxbp.flowabletree.operations.share.transitions import (
    CancelTransition,
)
from rxbp.flowabletree.operations.share.sharedmemory import ShareSharedMemory
from rxbp.flowabletree.operations.share.states import (
    ActiveState,
    CancelledState,
)


@dataclass(frozen=False)
class ShareCancellation(CancellationState):
    id: int
    shared: ShareSharedMemory
    upstream_cancellation: Cancellable
    _certificate: ContinuationCertificate

    def cancel(self, certificate: ContinuationCertificate):
        super().cancel(certificate)

        transition = CancelTransition(
            child=None,  # type: ignore
            id=self.id,
            certificate=certificate,
        )

        with self.shared.lock:
            transition.child = self.shared.transition
            self.shared.transition = transition

        match transition.get_state():
            case CancelledState(certificate=certificate):
                self.upstream_cancellation.cancel(certificate)

            case ActiveState():
                pass