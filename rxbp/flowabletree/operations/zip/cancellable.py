from dataclasses import dataclass

from continuationmonad.typing import ContinuationCertificate

from rxbp.cancellable import Cancellable
from rxbp.flowabletree.operations.zip.sharedmemory import ZipSharedMemory
from rxbp.flowabletree.operations.zip.states import CancelledState
from rxbp.flowabletree.operations.zip.statetransitions import CancelTransition


@dataclass
class ZipCancellable(Cancellable):
    cancellables: tuple[Cancellable]
    shared: ZipSharedMemory

    def cancel(self, certificate: ContinuationCertificate):
        transition = CancelTransition(
            child=None,  # type: ignore
            certificate=certificate,
            n_children=self.shared.n_children,
        )
        
        with self.shared.lock:
            transition.child = self.shared.transition
            self.shared.transition = transition

        match state := transition.get_state():
            case CancelledState(certificates=certificates):
                for id, certificate in certificates.items():
                    self.cancellables[id].cancel((certificate,))

            case _:
                Exception(f"Unexpected state {state}.")