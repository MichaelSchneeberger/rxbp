from rx import Observable
from rx.internal import extensionmethod

from rxbackpressure.core.anonymousbackpressureobservable import \
    AnonymousBackpressureObservable
from rxbackpressure.core.anonymoussubflowobservable import AnonymousSubFlowObservable
from rxbackpressure.core.backpressureobservable import BackpressureObservable
from rxbackpressure.core.subflowobservable import SubFlowObservable


def do_action_subscribe_func(self, observer, scheduler, on_next=None, on_error=None, on_completed=None):

    def on_next2(value):
        if on_next:
            on_next(value)
        observer.on_next(value)

    def on_error2(value):
        if on_error:
            on_error(value)
        observer.on_error(value)

    def on_completed2():
        if on_completed:
            on_completed()
        observer.on_completed()

    def subscribe_bp(backpressure):
        return observer.subscribe_backpressure(backpressure)

    return self.subscribe(on_next2, on_error2, on_completed=on_completed2, subscribe_bp=subscribe_bp,
                          scheduler=scheduler)


@extensionmethod(BackpressureObservable)
def do_action(self, on_next=None, on_error=None, on_completed=None):
    def subscribe_func(observer, scheduler):
        return do_action_subscribe_func(self, observer, scheduler, on_next, on_error, on_completed)

    return AnonymousBackpressureObservable(subscribe_func=subscribe_func, name='do_action')


@extensionmethod(SubFlowObservable)
def do_action(self, on_next=None, on_error=None, on_completed=None):
    def subscribe_func(observer, scheduler):
        return do_action_subscribe_func(self, observer, scheduler, on_next, on_error, on_completed)

    return AnonymousSubFlowObservable(subscribe_func=subscribe_func, name='do_action')