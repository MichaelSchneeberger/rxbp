from rx import AnonymousObservable
from rx.internal import extensionmethod

from rxbackpressure.backpressuretypes.stoprequest import StopRequest
from rxbackpressure.core.anonymousbackpressureobservable import \
    AnonymousBackpressureObservable
from rxbackpressure.core.backpressurebase import BackpressureBase
from rxbackpressure.core.backpressureobservable import BackpressureObservable


@extensionmethod(BackpressureObservable)
def pairwise(self):
    class PairwiseBackpressure(BackpressureBase):
        def __init__(self, backpressure):
            self.backpressure = backpressure
            self.is_first = True

        def request(self, number_of_items):
            # print('request pariwise {}'.format(number_of_items))
            if isinstance(number_of_items, StopRequest):
                f1 = self.backpressure.request(number_of_items)
            else:
                if self.is_first:
                    self.backpressure.request(1)
                    self.is_first = False
                f1 = self.backpressure.request(number_of_items)
            return f1

    def subscribe_func(observer, scheduler):
        parent_scheduler = scheduler
        def subscribe_bp(backpressure):
            parent_backpressure = PairwiseBackpressure(backpressure)
            return observer.subscribe_backpressure(parent_backpressure)

        obs1 = AnonymousObservable(subscribe=lambda o1: self.subscribe(observer=o1, subscribe_bp=subscribe_bp,
                                                                       scheduler=scheduler))
        disposable = obs1.pairwise().subscribe(observer)
        # print(disposable)
        return disposable

    obs = AnonymousBackpressureObservable(subscribe_func=subscribe_func, name='pairwise')
    return obs
