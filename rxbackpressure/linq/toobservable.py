from rx import AnonymousObservable
from rx.internal import extensionmethod

from rxbackpressure.backpressuretypes.backpressuregreadily import BackpressureGreadily
from rxbackpressure.core.backpressureobservable import BackpressureObservable


@extensionmethod(BackpressureObservable)
def to_observable(self):

    def subscribe(observer):
        def subscribe_bp(backpressure):
            # print('subscribe backpressure in to_observable')
            BackpressureGreadily.apply(backpressure=backpressure)

        # backpressure_observer = AnonymousBackpressureObserver(subscribe_bp=subscribe_bp, observer=observer)

        disposable = self.subscribe(subscribe_bp=subscribe_bp, observer=observer)
        # print(disposable)
        return disposable

    return AnonymousObservable(subscribe=subscribe)
