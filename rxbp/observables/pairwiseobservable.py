import itertools

from rxbp.ack import continue_ack, stop_ack
from rxbp.observable import Observable
from rxbp.observer import Observer


class PairwiseObservable(Observable):
    def __init__(self, source):
        self.source = source

    def observe(self, observer: Observer):

        last_elem = [None]

        def on_next(v):

            def pairwise_gen_template(iterator):
                for elem in iterator:
                    yield last_elem[0], elem
                    last_elem[0] = elem

            # is first element
            if last_elem[0] is None:

                # catches exceptions raised when consuming next element from iterator
                try:
                    temp_iter = v()

                    peak_first = next(temp_iter)

                    try:
                        peak_second = next(temp_iter)
                    except StopIteration:
                        last_elem[0] = peak_first
                        return continue_ack

                except Exception as exc:
                    observer.on_error(exc)
                    return stop_ack

                def pairwise_gen():
                    yield peak_first, peak_second
                    last_elem[0] = peak_second
                    yield from pairwise_gen_template(temp_iter)

            else:
                def pairwise_gen():
                    yield from pairwise_gen_template(v())

            ack = observer.on_next(pairwise_gen)
            return ack

        class PairwiseObserver(Observer):
            def on_next(self, v):
                return on_next(v)

            def on_error(self, exc):
                return observer.on_error(exc)

            def on_completed(self):
                return observer.on_completed()

        pairwise_observer = PairwiseObserver()
        return self.source.observe(pairwise_observer)
