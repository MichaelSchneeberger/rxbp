from rx import Observable
from rx.internal import extensionmethod

from rxbackpressure.core.anonymousbackpressureobservable import \
    AnonymousBackpressureObservable
from rxbackpressure.core.anonymoussubflowobservable import AnonymousSubFlowObservable
from rxbackpressure.core.backpressureobservable import BackpressureObservable
from rxbackpressure.core.subflowobservable import SubFlowObservable


def do_onnext_subscribe_func(self, observer, scheduler, func=None):

    def on_next2(value):
        if func:
            func(value)
        observer.on_next(value)


    def subscribe_bp(backpressure):
        return observer.subscribe_backpressure(backpressure)

    return self.subscribe(on_next2, observer.on_error, on_completed=observer.on_completed,
                          subscribe_bp=subscribe_bp,
                          scheduler=scheduler)


@extensionmethod(BackpressureObservable)
def do_onnext(self, func=None):
    def subscribe_func(observer, scheduler):
        return do_onnext_subscribe_func(self, observer, scheduler, func)
    return AnonymousBackpressureObservable(subscribe_func=subscribe_func)


@extensionmethod(SubFlowObservable)
def do_onnext(self, func=None):
    def subscribe_func(observer, scheduler):
        return do_onnext_subscribe_func(self, observer, scheduler, func)
    return AnonymousSubFlowObservable(subscribe_func=subscribe_func)