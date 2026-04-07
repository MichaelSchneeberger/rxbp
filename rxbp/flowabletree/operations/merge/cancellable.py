from dataclasses import dataclass

from contcert.typing import ContCert

from rxbp.cancellable import Cancellable
from rxbp.flowabletree.operations.merge.states import TerminatedStateMixin
from rxbp.flowabletree.operations.merge.statetransitions import CancelTransition
from rxbp.flowabletree.operations.merge.sharedmemory import MergeSharedMemory


@dataclass
class MergeCancellable(Cancellable):
    shared: MergeSharedMemory

    def cancel(self, certificate: ContCert):
        transition = CancelTransition(
            child=None,  # type: ignore
            certificate=certificate,
        )

        with self.shared.lock:
            transition.child = self.shared.transition
            self.shared.transition = transition

        match state := transition.get_state():
            case TerminatedStateMixin(
                certificates=certificates,
            ):
                for id, certificate in certificates.items():
                    self.shared.cancellables[id].cancel((certificate,))

            case _:
                raise Exception(f"Unexpected state {state}.")
