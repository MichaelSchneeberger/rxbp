from rx.disposable import Disposable

from rxbp.acknowledgement.ack import Ack
from rxbp.acknowledgement.single import Single


def _map(source: Ack, func) -> Ack:
    class MapAck(Ack):
        def subscribe(self, single: Single) -> Disposable:
            class MapSingle(Single):
                def on_next(self, value):
                    # try:
                    result = func(value)
                    # except Exception as err:
                    #     single.on_error(err)
                    # else:
                    single.on_next(result)

                def on_error(self, exc: Exception):
                    single.on_error(exc)

            return source.subscribe(MapSingle())
    return MapAck()
