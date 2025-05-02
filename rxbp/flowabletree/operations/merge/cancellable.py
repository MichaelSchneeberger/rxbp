from dataclasses import dataclass

from continuationmonad.typing import ContinuationCertificate

from rxbp.cancellable import Cancellable
from rxbp.flowabletree.operations.merge.statetransitions import CancelTransition
from rxbp.flowabletree.operations.merge.sharedmemory import MergeSharedMemory
from rxbp.flowabletree.operations.merge.states import UpstreamID


@dataclass
class MergeCancellable(Cancellable):
    cancellables: dict[UpstreamID, Cancellable]
    shared: MergeSharedMemory

    def cancel(self, certificate: ContinuationCertificate):
        transition = CancelTransition(
            child=None,  # type: ignore
            certificate=certificate,
        )

        with self.shared.lock:
            transition.child = self.shared.transition
            self.shared.transition = transition

        state = transition.get_state()

        for id, certificate in state.certificates.items():
            self.cancellables[id].cancel(certificate)
