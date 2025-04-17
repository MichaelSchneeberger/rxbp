from dataclasses import dataclass

from continuationmonad.typing import ContinuationCertificate

from rxbp.cancellable import CancellationState
from rxbp.flowabletree.operations.flatmap.actions import CancelAction
from rxbp.flowabletree.operations.flatmap.sharedmemory import FlatMapSharedMemory
from rxbp.flowabletree.operations.flatmap.states import CancelledState


@dataclass
class FlatMapCancellable(CancellationState):
    _certificate: ContinuationCertificate
    shared: FlatMapSharedMemory

    def cancel(self, certificate: ContinuationCertificate):
        action = CancelAction(
            child=None,  # type: ignore
            certificate=certificate,
        )

        with self.shared.lock:
            action.child = self.shared.action
            self.shared.action = action

        match action:
            case CancelledState(cancellable=cancellable):
                cancellable.cancel(certificate)
