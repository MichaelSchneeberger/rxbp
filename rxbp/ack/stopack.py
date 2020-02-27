from rx.disposable import Disposable

from rxbp.ack.mixins.ackmergemixin import AckMergeMixin
from rxbp.ack.mixins.ackmixin import AckMixin
from rxbp.ack.single import Single


class StopAck(AckMergeMixin, AckMixin):
    is_sync = True

    def subscribe(self, single: Single) -> Disposable:
        single.on_next(stop_ack)
        return Disposable()

    def merge(self, other: AckMixin):
        return self


stop_ack = StopAck()