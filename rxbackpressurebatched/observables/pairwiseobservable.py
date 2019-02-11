import itertools

from rx.concurrency.schedulerbase import SchedulerBase

from rxbackpressurebatched.ack import continue_ack
from rxbackpressurebatched.observable import Observable
from rxbackpressurebatched.observer import Observer


class PairwiseObservable(Observable):
    def __init__(self, source):
        self.source = source

    def unsafe_subscribe(self, observer: Observer, scheduler: SchedulerBase,
                         subscribe_scheduler: SchedulerBase):

        last_elem = [None]

        def on_next(v):
            if last_elem[0] is None:
                last_elem[0] = v()
            #     return continue_ack
                iter = last_elem[0]
            else:
                iter = itertools.chain(last_elem[0], v())

            a, b = itertools.tee(iter)

            def pairwise_gen():
                next(b, None)
                yield from zip(a, b)

            last_elem[0] = a

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
        return self.source.unsafe_subscribe(map_observer, scheduler, subscribe_scheduler)
