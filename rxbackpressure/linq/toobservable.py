from rx import AnonymousObservable
from rx.core import Disposable
from rx.internal import extensionmethod

from rxbackpressure.backpressuretypes.backpressuregreadily import BackpressureGreadily
from rxbackpressure.core.backpressureobservable import BackpressureObservable


@extensionmethod(BackpressureObservable)
def to_observable(self):

    def subscribe(observer):
        def subscribe_bp(backpressure, scheduler):
            # print('subscribe backpressure in to_observable')
            return BackpressureGreadily.apply(backpressure=backpressure, scheduler=scheduler)

        disposable = self.subscribe(subscribe_bp=subscribe_bp, observer=observer)
        return disposable

    return AnonymousObservable(subscribe=subscribe)
