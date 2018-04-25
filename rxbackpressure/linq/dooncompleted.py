from rx import Observable
from rx.internal import extensionmethod

from rxbackpressure.core.anonymousbackpressureobservable import \
    AnonymousBackpressureObservable
from rxbackpressure.core.backpressureobservable import BackpressureObservable
from rxbackpressure.core.subflowobservable import SubFlowObservable


def do_oncompleted_func(self, func=None):

    def subscribe_func(observer, scheduler):

        def on_completed2():
            if func:
                func()
            observer.on_completed()

        def subscribe_bp(backpressure):
            return observer.subscribe_backpressure(backpressure)

        return self.subscribe(observer.on_next, observer.on_error, on_completed=on_completed2, subscribe_bp=subscribe_bp,
                              scheduler=scheduler)

    return AnonymousBackpressureObservable(subscribe_func=subscribe_func)


@extensionmethod(BackpressureObservable)
def do_oncompleted(self, func=None):
    return do_oncompleted_func(self, func)


@extensionmethod(SubFlowObservable)
def do_oncompleted(self, func=None):
    return do_oncompleted_func(self, func)
