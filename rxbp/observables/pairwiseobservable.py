import itertools

from rxbp.ack.ackimpl import continue_ack, stop_ack
from rxbp.observable import Observable
from rxbp.observer import Observer
from rxbp.observerinfo import ObserverInfo
from rxbp.typing import ElementType


class PairwiseObservable(Observable):
    def __init__(self, source):
        self.source = source

    def observe(self, observer_info: ObserverInfo):
        observer = observer_info.observer

        class PairwiseObserver(Observer):
            def __init__(self):
                self.last_elem = None

            def on_next(self, elem: ElementType):
                def pairwise_gen_template(iterator):
                    for elem in iterator:
                        yield self.last_elem, elem
                        self.last_elem = elem

                # is first element
                if self.last_elem is None:

                    # catches exceptions raised when consuming next element from iterator
                    try:
                        temp_iter = iter(elem)

                        try:
                            peak_first = next(temp_iter)
                        except StopIteration:
                            return continue_ack

                        try:
                            peak_second = next(temp_iter)
                        except StopIteration:
                            self.last_elem = peak_first
                            return continue_ack

                    except Exception as exc:
                        observer.on_error(exc)
                        return stop_ack

                    def pairwise_gen():
                        yield peak_first, peak_second
                        self.last_elem = peak_second
                        yield from pairwise_gen_template(temp_iter)

                else:
                    def pairwise_gen():
                        yield from pairwise_gen_template(elem)

                ack = observer.on_next(pairwise_gen())
                return ack

            def on_error(self, exc):
                return observer.on_error(exc)

            def on_completed(self):
                return observer.on_completed()

        pairwise_subscription = observer_info.copy(PairwiseObserver())
        return self.source.observe(pairwise_subscription)
