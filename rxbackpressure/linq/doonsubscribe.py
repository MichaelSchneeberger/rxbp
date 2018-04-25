from rx.internal import extensionmethod

from rxbackpressure.core.anonymousbackpressureobservable import AnonymousBackpressureObservable
from rxbackpressure.core.anonymoussubflowobservable import AnonymousSubFlowObservable
from rxbackpressure.core.backpressureobservable import BackpressureObservable
from rxbackpressure.core.subflowobservable import SubFlowObservable


def do_onsubscribe_func(self, observer, scheduler, func=None):
    if func:
        func(observer)
    return self.subscribe(observer, scheduler)


@extensionmethod(BackpressureObservable)
def do_onsubscribe(self, func=None):
    def subscribe_func(observer, scheduler):
        return do_onsubscribe_func(self, observer, scheduler, func)

    return AnonymousBackpressureObservable(subscribe_func=subscribe_func)


@extensionmethod(SubFlowObservable)
def do_onsubscribe(self, func=None):
    def subscribe_func(observer, scheduler):
        return do_onsubscribe_func(self, observer, scheduler, func)

    return AnonymousSubFlowObservable(subscribe_func=subscribe_func)
