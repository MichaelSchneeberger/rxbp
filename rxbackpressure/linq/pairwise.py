from rx import AnonymousObservable
from rx.internal import extensionmethod

from rxbackpressure.core.anonymousbackpressureobservable import \
    AnonymousBackpressureObservable
from rxbackpressure.core.backpressurebase import BackpressureBase
from rxbackpressure.core.backpressureobservable import BackpressureObservable
from rxbackpressure.internal.blockingfuture import BlockingFuture


@extensionmethod(BackpressureObservable)
def pairwise(self):
    class PairwiseBackpressure(BackpressureBase):
        def __init__(self, backpressure):
            self.backpressure = backpressure
            self.is_first = True

        def request(self, number_of_items) -> BlockingFuture:
            if self.is_first:
                self.backpressure.request(1)
                self.is_first = False
            f1 = self.backpressure.request(number_of_items)
            return f1

    def subscribe_func(observer):
        def subscribe_bp(backpressure):
            parent_backpressure = PairwiseBackpressure(backpressure)
            observer.subscribe_backpressure(parent_backpressure)

        obs1 = AnonymousObservable(subscribe=lambda observer: self.subscribe(observer=observer, subscribe_bp=subscribe_bp))
        disposable = obs1.pairwise().subscribe(observer)
        # print(disposable)
        return disposable

    obs = AnonymousBackpressureObservable(subscribe_func=subscribe_func)
    return obs
