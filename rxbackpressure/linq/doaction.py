from rx import Observable
from rx.internal import extensionmethod

from rxbackpressure.core.anonymousbackpressureobservable import \
    AnonymousBackpressureObservable
from rxbackpressure.core.backpressureobservable import BackpressureObservable


@extensionmethod(BackpressureObservable)
def do_action(self, on_next):

    def subscribe_func(observer):

        def on_next_2(value):
            on_next(value)
            observer.on_next(result)

        def subscribe_bp(backpressure):
            return observer.subscribe_backpressure(backpressure)

        return self.subscribe(on_next_2, observer.on_error, observer.on_completed, subscribe_bp=subscribe_bp)

    return AnonymousBackpressureObservable(subscribe_func=subscribe_func)
