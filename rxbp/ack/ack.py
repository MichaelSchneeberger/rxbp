from rx.disposable import Disposable
from rxbp.ack.ackbase import AckBase
from rxbp.ack.ackmixin import AckMixin
from rxbp.ack.merge import _merge
from rxbp.ack.single import Single


class Ack(AckMixin, AckBase):
    pass
    # def __init__(self, ack: AckBase):
    #     self._ack = ack
    #
    # def subscribe(self, single: Single) -> Disposable:
    #     return self._ack.subscribe(single=single)
    #
    # def merge(self, other: AckBase):
    #     return Ack(_merge(self, other))
