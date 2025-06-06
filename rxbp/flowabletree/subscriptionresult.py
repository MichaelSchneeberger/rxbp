from dataclasses import dataclass

from continuationmonad.typing import ContinuationCertificate

from rxbp.cancellable import Cancellable


@dataclass
class SubscriptionResult:
    cancellable: Cancellable
    certificate: ContinuationCertificate


def init_subscription_result(
    cancellable: Cancellable,
    certificate: ContinuationCertificate,
):
    return SubscriptionResult(
        cancellable=cancellable,
        certificate=certificate,
    )
