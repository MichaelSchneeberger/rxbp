from rx.internal import noop, default_error

from rxbackpressure.core.backpressureobserver import BackpressureObserver


class AnonymousBackpressureObserver(BackpressureObserver):
    def __init__(self, on_next=None, on_error=None, on_completed=None, subscribe_bp=None, observer=None):
        super().__init__()

        if observer is not None or isinstance(on_next, BackpressureObserver):
            if observer is None:
                observer = on_next
            self._next = observer.on_next
            self._error = observer.on_error
            self._completed = observer.on_completed
            if subscribe_bp is None:
                self._subscribe_bp = observer.subscribe_backpressure
            else:
                self._subscribe_bp = subscribe_bp
        else:
            self._next = on_next or noop
            self._error = on_error or default_error
            self._completed = on_completed or noop
            self._subscribe_bp = subscribe_bp

    def subscribe_backpressure(self, backpressure, scheduler=None):
        return self._subscribe_bp(backpressure, scheduler=scheduler)

    def _on_next_core(self, value):
        self._next(value)

    def _on_error_core(self, error):
        self._error(error)

    def _on_completed_core(self):
        self._completed()