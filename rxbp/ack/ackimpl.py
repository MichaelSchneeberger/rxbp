from rx.disposable import Disposable
from rxbp.ack.ackbase import AckBase
from rxbp.ack.ackmixin import AckMixin
from rxbp.ack.single import Single


class Continue(AckMixin, AckBase):
    is_sync = True

    def subscribe(self, single: Single) -> Disposable:
        single.on_next(continue_ack)
        return Disposable()

    def merge(self, other: AckBase):
        return other

continue_ack = Continue()


class Stop(AckMixin, AckBase):
    is_sync = True

    def subscribe(self, single: Single) -> Disposable:
        single.on_next(stop_ack)
        return Disposable()

    def merge(self, other: AckBase):
        return self


stop_ack = Stop()