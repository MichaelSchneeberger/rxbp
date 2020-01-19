from rxbp.ack.continueack import ContinueAck
from rxbp.ack.mixins.ackmixin import AckMixin
from rxbp.ack.operators.map import _map
from rxbp.ack.operators.zip import _zip
from rxbp.ack.stopack import StopAck


def _merge(self, ack2: AckMixin) -> AckMixin:
    if isinstance(ack2, ContinueAck) or isinstance(self, StopAck):
        return_ack = self
    elif isinstance(ack2, StopAck) or isinstance(self, ContinueAck):
        return_ack = ack2
    else:

        def _(v1, v2):
            if isinstance(v1, StopAck) or isinstance(v2, StopAck):
                return StopAck()
            else:
                return ContinueAck()

        return_ack = _map(source=_zip(self, ack2), func=lambda t2: _(*t2))

    return return_ack