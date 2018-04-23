from rx import AnonymousObservable
from rx.core import Disposable
from rx.internal import extensionmethod

from rxbackpressure.backpressuretypes.backpressuregreadily import BackpressureGreadily
from rxbackpressure.core.backpressureobservable import BackpressureObservable


@extensionmethod(BackpressureObservable)
def to_observable(self, scheduler=None):
    parent_scheduler = scheduler

    def subscribe(observer):
        def subscribe_bp(backpressure, scheduler=None):
            # print('subscribe backpressure in to_observable')
            return BackpressureGreadily.apply(backpressure=backpressure, scheduler2=parent_scheduler)

        disposable = self.subscribe(subscribe_bp=subscribe_bp, observer=observer, scheduler=scheduler)
        return disposable

    return AnonymousObservable(subscribe=subscribe)
