from __future__ import annotations

from dataclasses import dataclass
from continuationmonad.typing import (
    ContinuationCertificate,
)

from rxbp.cancellable import Cancellable, CancellationState
from rxbp.flowabletree.operations.share.statetransitions import (
    CancelTransition,
)
from rxbp.flowabletree.operations.share.sharedmemory import ShareSharedMemory
from rxbp.flowabletree.operations.share.states import (
    RemoveBufferItemsStateMixin,
    CancelledState,
    TerminatedStateMixin,
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

            # match state := transition.get_state():
            #     case CancelledState():
            #         certificate = ContinuationCertificate.merge(
            #             tuple(state.requested_certificates.values())
            #         )
            #         state.certificate = certificate

        match state := transition.get_state():
            case CancelledState():
                self.upstream_cancellation.cancel(state.certificate)

            case RemoveBufferItemsStateMixin(n_buffer_pop=n_buffer_pop):
                # still active downstream observers remaining

                self.shared.pop_buffer(n_buffer_pop)

            case TerminatedStateMixin():
                pass
