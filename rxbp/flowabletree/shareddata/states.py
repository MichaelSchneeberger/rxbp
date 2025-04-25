from dataclasses import dataclass

from continuationmonad.typing import ContinuationCertificate


@dataclass
class StopContinuationMixin:
    certificate: ContinuationCertificate
