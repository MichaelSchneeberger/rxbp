
from __future__ import annotations

from dataclasses import dataclass
from typing import override

from contcert.typing import ContCert
from rxbp.cancellable import CancellationState
from rxbp.flowabletree.operations.share.sharedmemory import ShareSharedMemory


@dataclass(frozen=False)
class AsyncShareCancellation[U](CancellationState):
    id: int
    shared: ShareSharedMemory[U]
    # upstream_cancellation: Cancellable
    # _certificate: ContCert

    @override
    def cancel(self, certificate: ContCert):
        raise NotImplementedError
        # super().cancel(certificate)
        #
        # transition = CancelTransition(
        #     child=None,  # type: ignore
        #     id=self.id,
        #     certificate=certificate,
        # )
        #
        # with self.shared.transition_lock:
        #     transition.child = self.shared.transition
        #     self.shared.transition = transition
        #
        # match transition.get_state():
        #     case CancelledState(requested_certificates=requested_certificates):
        #         self.upstream_cancellation.cancel(
        #             tuple(requested_certificates.values())
        #         )
        #
        #     case RemoveBufferItemsStateMixin(n_buffer_pop=n_buffer_pop):
        #         # still active downstream observers remaining
        #
        #         self.shared.pop_buffer(n_buffer_pop)
        #
        #     case HasTerminatedState():
        #         pass
