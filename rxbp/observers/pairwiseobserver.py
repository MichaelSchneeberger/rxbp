import types

from rxbp.ack.continueack import continue_ack
from rxbp.ack.stopack import stop_ack
from rxbp.observer import Observer
from rxbp.typing import ElementType


class PairwiseObserver(Observer):
    def __init__(self, observer: Observer):
        self.observer = observer

        self.last_elem = None

    def pairwise_gen_template(self, iterator):
        for elem in iterator:
            yield self.last_elem, elem
            self.last_elem = elem

    def on_next(self, elem: ElementType):

        # replace on_next method for next `on_next` call
        def on_next_after_first(self, elem: ElementType):
            def pairwise_gen():
                yield from self.pairwise_gen_template(elem)

            ack = self.observer.on_next(pairwise_gen())
            return ack
        self.on_next = types.MethodType(on_next_after_first, self)

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
            self.observer.on_error(exc)
            return stop_ack

        def pairwise_gen():
            yield peak_first, peak_second
            self.last_elem = peak_second
            yield from self.pairwise_gen_template(temp_iter)

        ack = self.observer.on_next(pairwise_gen())
        return ack

    def on_error(self, exc):
        return self.observer.on_error(exc)

    def on_completed(self):
        return self.observer.on_completed()