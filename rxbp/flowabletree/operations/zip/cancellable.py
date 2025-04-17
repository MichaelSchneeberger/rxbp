from dataclasses import dataclass

from continuationmonad.typing import ContinuationCertificate

from rxbp.cancellable import Cancellable


@dataclass
class CompositeCancellable(Cancellable):
    cancellables: tuple[Cancellable]
    certificates: tuple[ContinuationCertificate]

    def cancel(self, certificate: ContinuationCertificate):
        certificates = self.certificates + (certificate,)

        for cancellable, c in zip(self.cancellables, certificates):
            cancellable.cancel(c)
