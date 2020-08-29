import threading
from dataclasses import dataclass
from typing import List, Callable, Any

import rx
from rx.disposable import CompositeDisposable, SingleAssignmentDisposable

from rxbp.acknowledgement.ack import Ack
from rxbp.acknowledgement.single import Single


def reduce_ack(
        sources: List[Ack],
        func: Callable[[Any, Any], Any],
        initial: Any,
) -> Ack:

    @dataclass
    class ReduceAck(Ack):
        sources: List[Ack]
        func: Callable[[Any, Any], Any]
        acc: [Any]

        def __post_init__(self):
            self.lock = threading.RLock()

        def subscribe(self, single: Single) -> rx.typing.Disposable:

            @dataclass
            class ReduceZipSinlge(Single):
                acc: [Any]
                func: Callable[[Any, Any], Any]
                lock: threading.RLock

                def on_next(self, elem):
                    with self.lock:
                        self.acc[0] = self.func(self.acc[0], elem)

            def gen_disposables():
                for source in self.sources:
                    yield source.subscribe(ReduceZipSinlge(
                        lock=self.lock,
                        func=self.func,
                        acc=self.acc,
                    ))

            return CompositeDisposable(list(gen_disposables()))

    return ReduceAck(
        sources=sources,
        func=func,
        acc=[initial],
    )
