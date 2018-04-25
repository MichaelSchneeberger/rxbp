from rx.disposables import SingleAssignmentDisposable

from rxbackpressure.core.backpressureobserver import BackpressureObserver


class AutoDetachBackpressureObserver(BackpressureObserver):

    def __init__(self, observer):
        super().__init__()

        self.observer = observer
        self.m = SingleAssignmentDisposable()

    def _on_next_core(self, value):
        try:
            self.observer.on_next(value)
        except Exception:
            self.dispose()
            raise

    def _on_error_core(self, exn):
        try:
            self.observer.on_error(exn)
        finally:
            self.dispose()

    def _on_completed_core(self):
        try:
            self.observer.on_completed()
        finally:
            self.dispose()

    def subscribe_backpressure(self, backpressure):
        # try:
        return self.observer.subscribe_backpressure(backpressure)
        # except Exception:
        #     self.dispose()

    def set_disposable(self, value):
        self.m.disposable = value

    disposable = property(fset=set_disposable)

    def dispose(self):
        super().dispose()
        self.m.dispose()
