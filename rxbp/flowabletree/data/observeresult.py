from typing import NamedTuple

from continuationmonad.typing import Cancellable, ContinuationCertificate


class ObserveResult(NamedTuple):
    cancellable: Cancellable
    certificate: ContinuationCertificate
