from rx import Observable
from rx.internal import extensionmethod

from rxbackpressure.core.anonymousbackpressureobservable import \
    AnonymousBackpressureObservable
from rxbackpressure.core.backpressureobservable import BackpressureObservable


@extensionmethod(BackpressureObservable)
def do_oncompleted(self, func=None):

    def subscribe_func(observer, scheduler=None):

        def on_completed2():
            # print(on_next)
            if func:
                func()
            observer.on_completed()

        def subscribe_bp(backpressure, scheduler=None):
            return observer.subscribe_backpressure(backpressure, scheduler)

        return self.subscribe(observer.on_next, observer.on_error, on_completed=on_completed2, subscribe_bp=subscribe_bp,
                              scheduler=scheduler)

    return AnonymousBackpressureObservable(subscribe_func=subscribe_func)
