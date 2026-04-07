from dataclasses import dataclass

from contcert.typing import ContCert

from rxbp.cancellable import Cancellable
from rxbp.flowabletree.operations.concatmap.statetransitions import CancelTransition
from rxbp.flowabletree.operations.concatmap.sharedmemory import ConcatMapSharedMemory
from rxbp.flowabletree.operations.concatmap.states import CancelledState


@dataclass
class ConcatMapCancellable(Cancellable):
    shared: ConcatMapSharedMemory

    def cancel(self, certificate: ContCert):
        transition = CancelTransition(
            child=None,  # type: ignore
            certificate=certificate,
        )

        with self.shared.lock:
            transition.child = self.shared.transition
            self.shared.transition = transition

        match transition:
            case CancelledState(cancellable=cancellable):
                cancellable.cancel((certificate,))
