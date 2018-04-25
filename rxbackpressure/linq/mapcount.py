from rx import Observable
from rx.internal import extensionmethod

from rxbackpressure.core.anonymousbackpressureobservable import \
    AnonymousBackpressureObservable
from rxbackpressure.core.backpressureobservable import BackpressureObservable


@extensionmethod(BackpressureObservable)
def map_count(self, selector):
    def subscribe_func(observer, scheduler):
        count = [0]
        parent_scheduler = scheduler

        def on_next(value):
            try:
                result = selector(value, count[0])
            except Exception as err:
                observer.on_error(err)
            else:
                count[0] += 1
                observer.on_next(result)

        def subscribe_bp(backpressure):
            return observer.subscribe_backpressure(backpressure)

        return self.subscribe(on_next, observer.on_error, observer.on_completed, subscribe_bp=subscribe_bp,
                              scheduler=scheduler)

    return AnonymousBackpressureObservable(subscribe_func=subscribe_func)
