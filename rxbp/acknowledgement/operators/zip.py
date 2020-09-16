import threading
from dataclasses import dataclass
from typing import List

from rx.disposable import CompositeDisposable, SingleAssignmentDisposable

from rxbp.acknowledgement.ack import Ack
from rxbp.acknowledgement.single import Single


def _zip(*args: Ack) -> Ack:
    sources = list(args)

    class ZipAck(Ack):
        def subscribe(self, single: Single):

            @dataclass
            class ZipSinlge(Single):
                idx: int
                queues: List[List]
                lock: threading.RLock

                def on_next(self, elem):
                    with self.lock:
                        self.queues[self.idx].append(elem)
                        send_values = all([len(q) for q in self.queues])

                    if send_values:
                        # try:
                        queued_values = [x.pop(0) for x in self.queues]
                        res = tuple(queued_values)
                        # except Exception as ex:
                        #     single.on_error(ex)
                        #     return

                        single.on_next(res)

                # def on_error(self, exc: Exception):
                #     single.on_error(exc)

            queues: List[List] = [[] for _ in sources]
            lock = threading.RLock()

            def gen_subscriptions():
                for idx, source in enumerate(sources):
                    yield source.subscribe(ZipSinlge(
                        idx=idx,
                        queues=queues,
                        lock=lock,
                    ))

            subscriptions = list(gen_subscriptions())

            return CompositeDisposable(subscriptions)

    return ZipAck()
