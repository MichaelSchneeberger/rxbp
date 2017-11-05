from rx import AnonymousObservable
from rx.internal import extensionmethod

from rxbackpressure.core.anonymousbackpressureobservable import \
    AnonymousBackpressureObservable
from rxbackpressure.core.backpressurebase import BackpressureBase
from rxbackpressure.core.backpressureobservable import BackpressureObservable
from rxbackpressure.internal.blockingfuture import BlockingFuture


@extensionmethod(BackpressureObservable)
def zip(self, other, selector=lambda v1, v2: (v1, v2)):
    class ZipBackpressure(BackpressureBase):
        def __init__(self, backpressure1, backpressure2):
            # print('create zip backpressure')
            self.backpressure1 = backpressure1
            self.backpressure2 = backpressure2

        def request(self, number_of_items) -> BlockingFuture:
            # print('zip request %s' % number_of_items)
            f1 = self.backpressure1.request(number_of_items)
            f2 = self.backpressure2.request(number_of_items)
            def selector(val):
                # print('selector')
                return min(val[0], val[1])
            return f1.zip(f2).map(selector)

    def subscribe_func(observer):
        backpressure1 = [None]
        backpressure2 = [None]

        def subscribe_bp():
            parent_backpressure = ZipBackpressure(backpressure1[0], backpressure2[0])
            observer.subscribe_backpressure(parent_backpressure)

        def subscribe_bp_1(backpressure):
            # print('subscribe backpressure 1')
            backpressure1[0] = backpressure

            if backpressure2[0] is not None:
                subscribe_bp()

        def subscribe_bp_2(backpressure):
            # print('subscribe backpressure 2')
            backpressure2[0] = backpressure

            if backpressure1[0] is not None:
                subscribe_bp()

        obs1 = AnonymousObservable(subscribe=lambda observer: self.subscribe(observer=observer, subscribe_bp=subscribe_bp_1))
        obs2 = AnonymousObservable(subscribe=lambda observer: other.subscribe(observer=observer, subscribe_bp=subscribe_bp_2))
        disposable = obs1.zip(obs2, selector).subscribe(observer)
        # print(disposable)
        return disposable

    obs = AnonymousBackpressureObservable(subscribe_func=subscribe_func)
    return obs
