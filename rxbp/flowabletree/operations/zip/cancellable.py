from dataclasses import dataclass
from typing import override

from contcert.typing import ContCert

from rxbp.cancellable import Cancellable
from rxbp.flowabletree.operations.zip.sharedmemory import ZipSharedMemory
from rxbp.flowabletree.operations.zip.states import CompletedState, ErrorState
from rxbp.flowabletree.operations.zip.statetransitions import CancelTransition


@dataclass
class ZipCancellable[U, V, E](Cancellable):
    cancellables: tuple[Cancellable, ...]
    shared: ZipSharedMemory[U, V, E]

    @override
    def cancel(self, certificate: ContCert):
        transition = CancelTransition(
            child=None,  # pyright: ignore[reportArgumentType]
        )

        with self.shared.transition_lock:
            transition.child = self.shared.transition
            self.shared.transition = transition

        match transition.get_state():
            case CompletedState(assigned_ids):
                certs = self.shared.certs + [certificate]
                for id, idx in assigned_ids.items():
                    self.cancellables[id].cancel(certs[idx])

            case ErrorState(msg):
                raise Exception(msg)
