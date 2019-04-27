import itertools

from rxbp.ack import continue_ack
from rxbp.observable import Observable
from rxbp.observer import Observer


class PairwiseObservable(Observable):
    def __init__(self, source):
        self.source = source

    def observe(self, observer: Observer):

        last_elem = [None]

        def on_next(v):
            if last_elem[0] is None:
                temp_iter = v()
                peak_first = next(temp_iter)
                try:
                    peak_second = next(temp_iter)
                except StopIteration:
                    last_elem[0] = [peak_first]
                    return continue_ack

                last_elem[0] = itertools.chain([peak_first, peak_second], last_elem[0])
                iter = last_elem[0]
            else:
                iter = itertools.chain(last_elem[0], v())

            a, b = itertools.tee(iter)

            def pairwise_gen():
                next(a, None)
                for e1, e2 in zip(a, b):
                    yield e2, e1

            last_elem[0] = b

            ack = observer.on_next(pairwise_gen)
            return ack

        class PairwiseObserver(Observer):
            def on_next(self, v):
                return on_next(v)

            def on_error(self, exc):
                return observer.on_error(exc)

            def on_completed(self):
                return observer.on_completed()

        map_observer = PairwiseObserver()
        return self.source.observe(map_observer,)
