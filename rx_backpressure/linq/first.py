from rx.internal import extensionmethod, SequenceContainsNoElementsError

from rx_backpressure.core.anonymous_backpressure_observable import \
    AnonymousBackpressureObservable
from rx_backpressure.core.backpressure_base import BackpressureBase
from rx_backpressure.core.backpressure_observable import BackpressureObservable
from rx_backpressure.internal.blocking_future import BlockingFuture


class FirstBackpressure(BackpressureBase):
    def __init__(self, backpressure):
        self.backpressure = backpressure
        self.is_send = False

    def request(self, number_of_items) -> BlockingFuture:
        if not self.is_send and number_of_items > 0:
            self.is_send = False
            return self.backpressure.request(1)
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
                observer.on_next(default_value)
                observer.on_completed()

        def subscribe_pb(backpressure):
            observer.subscribe_backpressure(FirstBackpressure(backpressure))

        return source.subscribe(on_next, observer.on_error, on_completed, subscribe_bp=subscribe_pb)
    return AnonymousBackpressureObservable(subscribe)


@extensionmethod(BackpressureObservable)
def first(self, predicate=None, default_value=None):
    return first_or_default_async(self, True, default_value)
