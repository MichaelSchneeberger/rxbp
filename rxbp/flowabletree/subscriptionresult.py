from dataclasses import dataclass

from contcert.typing import ContCert

from rxbp.cancellable import Cancellable


@dataclass
class SubscriptionResult:
    cancellable: Cancellable
    certificate: ContCert


def init_subscription_result(
    cancellable: Cancellable,
    certificate: ContCert,
):
    return SubscriptionResult(
        cancellable=cancellable,
        certificate=certificate,
    )
