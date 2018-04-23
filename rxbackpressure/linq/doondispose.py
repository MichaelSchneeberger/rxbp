from rx import Observable
from rx.core import Disposable
from rx.disposables import CompositeDisposable
from rx.internal import extensionmethod

from rxbackpressure.core.anonymousbackpressureobservable import \
    AnonymousBackpressureObservable
from rxbackpressure.core.anonymoussubflowobservable import AnonymousSubFlowObservable
from rxbackpressure.core.backpressureobservable import BackpressureObservable
from rxbackpressure.core.subflowobservable import SubFlowObservable


def do_ondispose_subscribe_func(self, observer, scheduler, func=None):

    def subscribe_bp(backpressure, scheduler=None):
        return observer.subscribe_backpressure(backpressure, scheduler)

    disposable = self.subscribe(on_next=observer.on_next, on_error=observer.on_error, on_completed=observer.on_completed,
                          subscribe_bp=subscribe_bp,
                          scheduler=scheduler)
    return CompositeDisposable(disposable, Disposable.create(func))

@extensionmethod(BackpressureObservable)
def do_ondispose(self, func=None):
    def subscribe_func(observer, scheduler):
        return do_ondispose_subscribe_func(self, observer, scheduler, func=func)


    return AnonymousBackpressureObservable(subscribe_func=subscribe_func, name='do_ondispose')


@extensionmethod(SubFlowObservable)
def do_ondispose(self, func=None):
    def subscribe_func(observer, scheduler):
        return do_ondispose_subscribe_func(self, observer, scheduler, func=func)

    return AnonymousSubFlowObservable(subscribe_func=subscribe_func, name='do_ondispose')