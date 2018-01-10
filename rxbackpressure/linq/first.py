from rx.internal import extensionmethod, SequenceContainsNoElementsError

from rxbackpressure.backpressuretypes.stoprequest import StopRequest
from rxbackpressure.core.anonymousbackpressureobservable import \
    AnonymousBackpressureObservable
from rxbackpressure.core.backpressurebase import BackpressureBase
from rxbackpressure.core.backpressureobservable import BackpressureObservable
from rxbackpressure.internal.blockingfuture import BlockingFuture


class FirstBackpressure(BackpressureBase):
    def __init__(self, backpressure):
        self.backpressure = backpressure
        self.is_send = False

    def request(self, number_of_items) -> BlockingFuture:
        if not self.is_send and number_of_items > 0:
            self.is_send = True
            future = self.backpressure.request(1)
            self.backpressure.request(StopRequest())
            return future
        else:
            f1 = BlockingFuture()
            f1.set(0)
            return f1


def first_or_default_async(source, has_default=False, default_value=None):
    def subscribe(observer):
        def on_next(x):
            observer.on_next(x)
            observer.on_completed()

        def on_completed():
            if not has_default:
                observer.on_error(SequenceContainsNoElementsError())
            else:
                if False:
                    observer.on_next(default_value)
                observer.on_completed()

        def subscribe_pb(backpressure):
            observer.subscribe_backpressure(FirstBackpressure(backpressure))

        return source.subscribe(on_next, observer.on_error, on_completed, subscribe_bp=subscribe_pb)
    return AnonymousBackpressureObservable(subscribe)


@extensionmethod(BackpressureObservable)
def first(self, predicate=None, default_value=None):
    return first_or_default_async(self, True, default_value)
