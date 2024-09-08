from dataclasses import dataclass
from continuationmonad.abc import Cancellable
from continuationmonad.schedulers.data.continuationcertificate import ContinuationCertificate


@dataclass
class CompositeCancellable(Cancellable):
    cancellables: tuple[Cancellable]
    certificates: tuple[ContinuationCertificate]

    def cancel(self, certificate: ContinuationCertificate):
        certificates = self.certificates + (certificate,)

        for cancellable, c in zip(self.cancellables, certificates):
            cancellable.cancel(c)