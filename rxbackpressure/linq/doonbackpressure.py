from rx import Observable
from rx.internal import extensionmethod

from rxbackpressure.core.anonymousbackpressureobservable import \
    AnonymousBackpressureObservable
from rxbackpressure.core.backpressurebase import BackpressureBase
from rxbackpressure.core.backpressureobservable import BackpressureObservable


@extensionmethod(BackpressureObservable)
def do_onbackpressure(self, func=None):

    def subscribe_func(observer, scheduler=None):

        def subscribe_bp(backpressure, scheduler=None):

            class Backpressure(BackpressureBase):
                def request(self, number_of_items):
                    func(number_of_items)
                    return backpressure.request(number_of_items)

            return observer.subscribe_backpressure(Backpressure(), scheduler)

        return self.subscribe(observer.on_next, observer.on_error, on_completed=observer.on_completed,
                              subscribe_bp=subscribe_bp,
                              scheduler=scheduler)

    return AnonymousBackpressureObservable(subscribe_func=subscribe_func)
