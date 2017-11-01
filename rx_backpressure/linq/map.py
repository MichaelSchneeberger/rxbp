from rx import Observable
from rx.internal import extensionmethod

from rx_backpressure.core.anonymous_backpressure_observable import \
    AnonymousBackpressureObservable
from rx_backpressure.core.backpressure_observable import BackpressureObservable


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

    def subscribe_func(observer):
        count = [0]

        def on_next(value):
            try:
                result = selector(value, count[0])
            except Exception as err:
                observer.on_error(err)
            else:
                count[0] += 1
                observer.on_next(result)

        def subscribe_bp(backpressure):
            return observer.subscribe_backpressure(backpressure)

        return self.subscribe(on_next, observer.on_error, observer.on_completed, subscribe_bp=subscribe_bp)

    return AnonymousBackpressureObservable(subscribe_func=subscribe_func)
