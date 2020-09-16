from dataclasses import dataclass

from rx.disposable import Disposable

from rxbp.acknowledgement.mixins.ackmergemixin import AckMergeMixin
from rxbp.acknowledgement.ack import Ack
from rxbp.acknowledgement.single import Single


@dataclass(frozen=True)
class StopAck(AckMergeMixin, Ack):
    is_sync = True

    def subscribe(self, single: Single) -> Disposable:
        single.on_next(stop_ack)
        return Disposable()

    def merge(self, other: Ack):
        return self


stop_ack = StopAck()