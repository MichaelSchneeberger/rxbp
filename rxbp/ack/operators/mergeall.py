import threading

from rx.disposable import CompositeDisposable, SingleAssignmentDisposable

from rxbp.ack.mixins.ackmixin import AckMixin
from rxbp.ack.single import Single


def _merge_all(source: AckMixin):

    class MergeAllAck(AckMixin):
        def subscribe(self, single: Single):
            group = CompositeDisposable()
            is_stopped = [False]
            m = SingleAssignmentDisposable()
            group.add(m)
            lock = threading.RLock()

            class MergeAllSingle(Single):
                def on_error(self, exc: Exception):
                    single.on_error(exc)

                def on_next(_, inner_source: Single):
                    inner_subscription = SingleAssignmentDisposable()
                    group.add(inner_subscription)

                    class ResultSingle(Single):
                        def on_next(self, elem):
                            single.on_next(elem)

                        def on_error(self, exc: Exception):
                            single.on_error(exc)

                    subscription = inner_source.subscribe(ResultSingle())
                    inner_subscription.disposable = subscription

            m.disposable = source.subscribe(MergeAllSingle())
            return group

    return MergeAllAck()