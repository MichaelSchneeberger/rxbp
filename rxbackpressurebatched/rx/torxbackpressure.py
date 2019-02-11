import rx

from rx.internal import extensionmethod

from rxbackpressurebatched.observers.bufferedsubscriber import BufferedSubscriber
from rxbackpressurebatched.observable import Observable
from rxbackpressurebatched.observableop import ObservableOp


@extensionmethod(rx.Observable, instancemethod=True)
def to_rxbackpressurebatched(self):
    source = self

    class ToBackpressureObservable(Observable):

        def unsafe_subscribe(self, observer, scheduler, subscribe_scheduler):
            subscriber = BufferedSubscriber(observer, scheduler, 1000)
            disposable = source.subscribe(on_next=subscriber.on_next, on_error=subscriber.on_error,
                                          on_completed=subscriber.on_completed)
            return disposable

    return ObservableOp(ToBackpressureObservable())


