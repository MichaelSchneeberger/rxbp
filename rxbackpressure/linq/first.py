from rx.internal import extensionmethod, SequenceContainsNoElementsError
from rx.subjects import AsyncSubject

from rxbackpressure.backpressuretypes.stoprequest import StopRequest
from rxbackpressure.core.anonymousbackpressureobservable import \
    AnonymousBackpressureObservable
from rxbackpressure.core.backpressurebase import BackpressureBase
from rxbackpressure.core.backpressureobservable import BackpressureObservable


class FirstBackpressure(BackpressureBase):
    def __init__(self, backpressure):
        self.backpressure = backpressure
        self.is_send = False

    def request(self, number_of_items):
        if not self.is_send and number_of_items > 0:
            self.is_send = True
            future = self.backpressure.request(1)
            result = future.flat_map(lambda v, idx: self.backpressure.request(StopRequest()))
            return result
        else:
            f1 = AsyncSubject()
            f1.on_next(0)
            f1.on_completed()
            return f1


def first_or_default_async(source, has_default=False, default_value=None):
    def subscribe(observer, scheduler):
        parent_scheduler = scheduler

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

        return source.subscribe(on_next, observer.on_error, on_completed, subscribe_bp=subscribe_pb,
                                scheduler=scheduler)
    return AnonymousBackpressureObservable(subscribe)


@extensionmethod(BackpressureObservable)
def first(self, predicate=None, default_value=None):
    return first_or_default_async(self, True, default_value)
