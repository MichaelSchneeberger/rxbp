from rx import AnonymousObservable, Observable
from rx.internal import extensionmethod

from rxbackpressure.core.anonymousbackpressureobservable import \
    AnonymousBackpressureObservable
from rxbackpressure.core.backpressureobservable import BackpressureObservable
from rxbackpressure.core.generatorobservable import GeneratorBackpressure


@extensionmethod(Observable)
def repeat_first(self):
    def subscribe_func(observer, scheduler):
        backpressure = GeneratorBackpressure(observer, scheduler=scheduler)
        observer.subscribe_backpressure(backpressure, scheduler)
        self.subscribe(on_next=lambda v: backpressure.set_future_value(v),
                       on_error=observer.on_error)

    obs = AnonymousBackpressureObservable(subscribe_func=subscribe_func, name='repeat_first')
    return obs
