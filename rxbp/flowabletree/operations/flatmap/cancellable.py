from dataclasses import dataclass

from continuationmonad.typing import ContinuationCertificate

from rxbp.cancellable import Cancellable
from rxbp.flowabletree.operations.flatmap.statetransitions import CancelTransition
from rxbp.flowabletree.operations.flatmap.sharedmemory import FlatMapSharedMemory
from rxbp.flowabletree.operations.flatmap.states import CancelledState


@dataclass
class FlatMapCancellable(Cancellable):
    shared: FlatMapSharedMemory

    def cancel(self, certificate: ContinuationCertificate):
        transition = CancelTransition(
            child=None,  # type: ignore
            certificate=certificate,
        )

        with self.shared.lock:
            transition.child = self.shared.transition
            self.shared.transition = transition

        match transition:
            case CancelledState(cancellable=cancellable):
                cancellable.cancel(certificate)
