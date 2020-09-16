import threading

from rx.disposable import CompositeDisposable, SingleAssignmentDisposable

from rxbp.acknowledgement.ack import Ack
from rxbp.acknowledgement.single import Single


def _merge_all(source: Ack):
    class MergeAllAck(Ack):
        def subscribe(self, single: Single):
            group = CompositeDisposable()
            m = SingleAssignmentDisposable()
            group.add(m)
            lock = threading.RLock()

            class MergeAllSingle(Single):
                # def on_error(self, exc: Exception):
                #     single.on_error(exc)

                def on_next(_, inner_source: Ack):
                    class ResultSingle(Single):
                        def on_next(self, elem):
                            single.on_next(elem)

                        # def on_error(self, exc: Exception):
                        #     single.on_error(exc)

                    disposable = inner_source.subscribe(ResultSingle())
                    group.add(disposable)

            m.disposable = source.subscribe(MergeAllSingle())
            return group

    return MergeAllAck()