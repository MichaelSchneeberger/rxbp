from rxbp.observer import Observer


class AnonymousObserver(Observer):
    def __init__(self, on_next_func, on_error_func, on_completed_func):
        self._on_next_func = on_next_func
        self._on_error_func = on_error_func
        self._on_completed_func = on_completed_func

    def on_next(self, value):
        return self._on_next_func(value)

    def on_error(self, error):
        return self._on_error_func(error)

    def on_completed(self):
        return self._on_completed_func()
