from rx import Observable
from rx.internal import extensionmethod

from rxbackpressure.core.anonymousbackpressureobservable import \
    AnonymousBackpressureObservable
from rxbackpressure.core.anonymoussubflowobservable import AnonymousSubFlowObservable
from rxbackpressure.core.backpressurebase import BackpressureBase
from rxbackpressure.core.backpressureobservable import BackpressureObservable
from rxbackpressure.core.subflowobservable import SubFlowObservable


def do_onsubscribebp_func(self, observer, scheduler, func=None):

    def subscribe_bp(backpressure):
        if func:
            func(backpressure)
        return observer.subscribe_backpressure(backpressure)

    return self.subscribe(observer.on_next, observer.on_error, on_completed=observer.on_completed,
                          subscribe_bp=subscribe_bp,
                          scheduler=scheduler)


@extensionmethod(BackpressureObservable)
def do_onsubscribebp(self, func=None):
    def subscribe_func(observer, scheduler):
        return do_onsubscribebp_func(self, observer, scheduler, func)

    return AnonymousBackpressureObservable(subscribe_func=subscribe_func)


@extensionmethod(SubFlowObservable)
def do_onsubscribebp(self, func=None):
    def subscribe_func(observer, scheduler):
        return do_onsubscribebp_func(self, observer, scheduler, func)

    return AnonymousSubFlowObservable(subscribe_func=subscribe_func)
