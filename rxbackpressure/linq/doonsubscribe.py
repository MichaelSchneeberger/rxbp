from rx.internal import extensionmethod

from rxbackpressure.core.anonymousbackpressureobservable import AnonymousBackpressureObservable
from rxbackpressure.core.backpressureobservable import BackpressureObservable
from rxbackpressure.core.subflowobservable import SubFlowObservable


def do_onsubscribe_func(self, func):

    def subscribe_func(observer, scheduler):
        func(observer)
        self.subscribe(observer, scheduler)

    return AnonymousBackpressureObservable(subscribe_func)


@extensionmethod(BackpressureObservable)
def do_onsubscribe(self, func):
    return do_onsubscribe_func(self, func)


@extensionmethod(SubFlowObservable)
def do_onsubscribe(self, func):
    return do_onsubscribe_func(self, func)
