from rxbp.ack.ackbase import AckBase
from rxbp.ack.ackimpl import Continue, Stop
from rxbp.ack.map import _map
from rxbp.ack.zip import _zip


def _merge(self, ack2: AckBase) -> AckBase:
    if isinstance(ack2, Continue) or isinstance(self, Stop):
        return_ack = self
    elif isinstance(ack2, Stop) or isinstance(self, Continue):
        return_ack = ack2
    else:

        def _(v1, v2):
            if isinstance(v1, Stop) or isinstance(v2, Stop):
                return Stop()
            else:
                return Continue()

        return_ack = _map(source=_zip(self, ack2), func=lambda t2: _(*t2))

    return return_ack