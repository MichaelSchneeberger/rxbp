from rx import Observable
from rx.internal import extensionmethod

from rxbackpressure.core.anonymousbackpressureobservable import \
    AnonymousBackpressureObservable
from rxbackpressure.core.backpressurebase import BackpressureBase
from rxbackpressure.core.backpressureobservable import BackpressureObservable
from rxbackpressure.core.subflowobservable import SubFlowObservable


def do_onsubscribebp_func(self, func=None):

    def subscribe_func(observer, scheduler):

        def subscribe_bp(backpressure, scheduler=None):
            func(backpressure)
            return observer.subscribe_backpressure(backpressure, scheduler)

        return self.subscribe(observer.on_next, observer.on_error, on_completed=observer.on_completed,
                              subscribe_bp=subscribe_bp,
                              scheduler=scheduler)

    return AnonymousBackpressureObservable(subscribe_func=subscribe_func)


@extensionmethod(BackpressureObservable)
def do_onsubscribebp(self, func=None):
    return do_onsubscribebp_func(self, func)


@extensionmethod(SubFlowObservable)
def do_onsubscribebp(self, func=None):
    return do_onsubscribebp_func(self, func)
