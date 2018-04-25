from rx import Observable
from rx.internal import extensionmethod

from rxbackpressure.core.anonymousbackpressureobservable import \
    AnonymousBackpressureObservable
from rxbackpressure.core.backpressureobservable import BackpressureObservable


@extensionmethod(BackpressureObservable)
def map(self, selector):
    """Project each element of an observable sequence into a new form
    by incorporating the element's index.

    1 - source.map(lambda value: value * value)
    2 - source.map(lambda value, index: value * value + index)

    Keyword arguments:
    :param Callable[[Any, Any], Any] selector: A transform function to
        apply to each source element; the second parameter of the
        function represents the index of the source element.
    :rtype: Observable

    Returns an observable sequence whose elements are the result of
    invoking the transform function on each element of source.
    """

    def subscribe_func(observer, scheduler):
        parent_scheduler = scheduler

        def on_next(value):
            try:
                result = selector(value)
            except Exception as err:
                observer.on_error(err)
            else:
                observer.on_next(result)

        def subscribe_bp(backpressure):
            return observer.subscribe_backpressure(backpressure)

        return self.subscribe(on_next, observer.on_error, observer.on_completed, subscribe_bp=subscribe_bp,
                              scheduler=scheduler)

    return AnonymousBackpressureObservable(subscribe_func=subscribe_func)
