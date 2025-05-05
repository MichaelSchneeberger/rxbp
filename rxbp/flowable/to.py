from continuationmonad.typing import (
    MainScheduler,
)

from rxbp.flowabletree.to import (
    run as _run,
    to_rx as _to_rx,
)
from rxbp.flowable.flowable import ConnectableFlowable, Flowable


def run[U](
    source: Flowable[U],
    scheduler: MainScheduler | None = None,
    connections: dict[ConnectableFlowable, Flowable] | None = None,
):
    match connections:
        case None:
            _connections = connections
        case _:
            _connections = {c.child: s.child for c, s in connections.items()}

    return _run(
        source=source.child,
        scheduler=scheduler, 
        connections=_connections,
    )


def to_rx[U](source: Flowable[U]):
    return _to_rx(source)
