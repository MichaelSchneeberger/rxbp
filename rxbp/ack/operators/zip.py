from typing import List

from rx.disposable import CompositeDisposable, SingleAssignmentDisposable

from rxbp.ack.mixins.ackmixin import AckMixin
from rxbp.ack.single import Single


def _zip(*args: AckMixin) -> AckMixin:
    sources = list(args)

    class ZipAck(AckMixin):
        def subscribe(self, single: Single):
            n = len(sources)
            queues: List[List] = [[] for _ in range(n)]

            def next():
                if all([len(q) for q in queues]):
                    try:
                        queued_values = [x.pop(0) for x in queues]
                        res = tuple(queued_values)
                    except Exception as ex:
                        single.on_error(ex)
                        return

                    single.on_next(res)

            subscriptions = [None]*n

            def func(i):
                source = sources[i]
                sad = SingleAssignmentDisposable()

                class ZipSinlge(Single):
                    def on_next(self, elem):
                        queues[i].append(elem)
                        next()

                    def on_error(self, exc: Exception):
                        single.on_error(exc)

                sad.disposable = source.subscribe(ZipSinlge())
                subscriptions[i] = sad

            for idx in range(n):
                func(idx)
            return CompositeDisposable(subscriptions)
    return ZipAck()
