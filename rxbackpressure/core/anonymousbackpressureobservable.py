from rxbackpressure.core.backpressureobservable import BackpressureObservable


class AnonymousBackpressureObservable(BackpressureObservable):
    """Class to create an Observable instance from a delegate-based
    implementation of the Subscribe method."""

    def __init__(self, subscribe_func):
        """Creates an observable sequence object from the specified
        subscription function.

        Keyword arguments:
        :param types.FunctionType subscribe: Subscribe method implementation.
        """

        self._subscribe_func = subscribe_func
        # self._subscribe_backpressure_func = subscribe_backpressure_func
        super().__init__()

    def _subscribe_core(self, observer):
        return self._subscribe_func(observer)
