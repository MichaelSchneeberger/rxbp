from rxbackpressure.observer import Observer


class AnonymousObserver(Observer):
    def __init__(self, on_next, on_error, on_completed):
        self._on_next = on_next
        self._on_error = on_error
        self._on_completed = on_completed

    def on_next(self, value):
        return self._on_next(value)

    def on_error(self, error):
        return self._on_error(error)

    def on_completed(self):
        return self._on_completed()
