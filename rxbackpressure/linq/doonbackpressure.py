from rx import Observable
from rx.internal import extensionmethod

from rxbackpressure.core.anonymousbackpressureobservable import \
    AnonymousBackpressureObservable
from rxbackpressure.core.anonymoussubflowobservable import AnonymousSubFlowObservable
from rxbackpressure.core.backpressurebase import BackpressureBase
from rxbackpressure.core.backpressureobservable import BackpressureObservable
from rxbackpressure.core.subflowobservable import SubFlowObservable


def do_onbackpressure_func(self, observer, scheduler, func):

    # def subscribe_func(observer, scheduler):

    def subscribe_bp(backpressure, scheduler=None):

        class Backpressure(BackpressureBase):
            def request(self, number_of_items):
                func(number_of_items)
                return backpressure.request(number_of_items)

        return observer.subscribe_backpressure(Backpressure(), scheduler)

    return self.subscribe(observer.on_next, observer.on_error, on_completed=observer.on_completed,
                          subscribe_bp=subscribe_bp,
                          scheduler=scheduler)

    # return AnonymousBackpressureObservable(subscribe_func=subscribe_func)


@extensionmethod(BackpressureObservable)
def do_onbackpressure(self, func=None):
    def subscribe_func(observer, scheduler):
        return do_onbackpressure_func(self, observer, scheduler, func)

    return AnonymousBackpressureObservable(subscribe_func=subscribe_func, name='do_onbackpressure')


@extensionmethod(SubFlowObservable)
def do_onbackpressure(self, func=None):
    def subscribe_func(observer, scheduler):
        return do_onbackpressure_func(self, observer, scheduler, func)

    return AnonymousSubFlowObservable(subscribe_func=subscribe_func, name='do_onbackpressure')
