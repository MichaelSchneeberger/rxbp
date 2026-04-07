from dataclasses import dataclass
from typing import override

from contcert.typing import ContCert

from rxbp.cancellable import Cancellable
from rxbp.flowabletree.operations.zip.sharedmemory import ZipSharedMemory
from rxbp.flowabletree.operations.zip.states import Completed, Error
from rxbp.flowabletree.operations.zip.statetransitions import CancelTransition


@dataclass
class ZipCancellable[U, A](Cancellable):
    cancellables: tuple[Cancellable, ...]
    shared: ZipSharedMemory[U, A]

    @override
    def cancel(self, certificate: ContCert):
        transition = CancelTransition(
            child=None,  # pyright: ignore[reportArgumentType]
        )

        with self.shared.transition_lock:
            transition.child = self.shared.transition
            self.shared.transition = transition

        match transition.get_state():
            case Completed(assigned_ids):
                certs = self.shared.certs + [certificate]
                for id, idx in assigned_ids.items():
                    self.cancellables[id].cancel(certs[idx])

            case Error(msg):
                raise Exception(msg)
