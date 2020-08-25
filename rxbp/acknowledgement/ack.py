from abc import ABC

from rxbp.acknowledgement.mixins.ackmixin import AckMixin


class Ack(AckMixin, ABC):
    pass
