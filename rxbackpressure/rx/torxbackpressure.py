import rx

from rx.internal import extensionmethod

from rxbackpressure.observers.bufferedsubscriber import BufferedSubscriber
from rxbackpressure.observable import Observable
from rxbackpressure.observableop import ObservableOp


@extensionmethod(rx.Observable, instancemethod=True)
def to_rxbackpressure(self):
    source = self

    class ToBackpressureObservable(Observable):

        def unsafe_subscribe(self, observer, scheduler, subscribe_scheduler):
            subscriber = BufferedSubscriber(observer, scheduler, 1000)
            disposable = source.subscribe(on_next=subscriber.on_next, on_error=subscriber.on_error,
                                          on_completed=subscriber.on_completed)
            return disposable

    return ObservableOp(ToBackpressureObservable())


