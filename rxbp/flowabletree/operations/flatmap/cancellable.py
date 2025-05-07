from dataclasses import dataclass

from continuationmonad.typing import ContinuationCertificate

from rxbp.cancellable import Cancellable
from rxbp.flowabletree.operations.flatmap.states import TerminatedStateMixin
from rxbp.flowabletree.operations.flatmap.statetransitions import CancelTransition
from rxbp.flowabletree.operations.flatmap.sharedmemory import FlatMapSharedMemory


@dataclass
class FlatMapCancellable(Cancellable):
    upstream: Cancellable
    shared: FlatMapSharedMemory

    def cancel(self, certificate: ContinuationCertificate):
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
                outer_certificate=outer_certificate,
            ):
                self.upstream.cancel(outer_certificate)

                for id, certificate in certificates.items():
                    self.shared.cancellables[id].cancel(certificate)

            case _:
                raise Exception(f"Unexpected state {state}.")
