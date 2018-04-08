from rx import Observable
from rx.internal import extensionmethod

from rxbackpressure.core.anonymousbackpressureobservable import \
    AnonymousBackpressureObservable
from rxbackpressure.core.backpressureobservable import BackpressureObservable


@extensionmethod(BackpressureObservable)
def do_onnext(self, func=None):

    def subscribe_func(observer, scheduler=None):
        parent_scheduler = scheduler

        def on_next2(value):
            if func:
                func(value)
            observer.on_next(value)


        def subscribe_bp(backpressure, scheduler=None):
            return observer.subscribe_backpressure(backpressure, parent_scheduler)

        return self.subscribe(on_next2, observer.on_error, on_completed=observer.on_completed,
                              subscribe_bp=subscribe_bp,
                              scheduler=scheduler)

    return AnonymousBackpressureObservable(subscribe_func=subscribe_func)
