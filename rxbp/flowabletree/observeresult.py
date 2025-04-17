from dataclasses import dataclass

from continuationmonad.typing import ContinuationCertificate

from rxbp.cancellable import Cancellable


@dataclass
class ObserveResult:
    cancellable: Cancellable
    certificate: ContinuationCertificate
