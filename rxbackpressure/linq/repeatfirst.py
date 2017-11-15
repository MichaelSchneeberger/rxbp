from rx import AnonymousObservable, Observable
from rx.internal import extensionmethod

from rxbackpressure.core.anonymousbackpressureobservable import \
    AnonymousBackpressureObservable
from rxbackpressure.core.backpressureobservable import BackpressureObservable
from rxbackpressure.core.generatorobservable import GeneratorBackpressure


@extensionmethod(Observable)
def repeat_first(self):
    def subscribe_func(observer):
        backpressure = GeneratorBackpressure(observer)
        observer.subscribe_backpressure(backpressure)
        self.subscribe(on_next=lambda v: backpressure.set_future_value(v),
                       on_error=observer.on_error)

    obs = AnonymousBackpressureObservable(subscribe_func=subscribe_func)
    return obs

# @extensionmethod(BackpressureObservable)
# def repeat_first(self):
#     def subscribe_func(observer):
#         def subscribe_bp(backpressure):
#             backpressure.request(1)
#
#         backpressure = GeneratorBackpressure(observer)
#         observer.subscribe_backpressure(backpressure)
#         self.subscribe(on_next=lambda v: backpressure.set_future_value(v),
#                        on_error=observer.on_error,
#                        subscribe_bp=subscribe_bp)
#
#         # obs1 = AnonymousObservable(subscribe=lambda o1: self.subscribe(observer=o1, subscribe_bp=subscribe_bp))
#         # disposable = obs1.pairwise().subscribe(observer)
#         # # print(disposable)
#         # return disposable
#
#     obs = AnonymousBackpressureObservable(subscribe_func=subscribe_func)
#     return obs
