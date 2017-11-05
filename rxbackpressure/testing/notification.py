from rx.core.notification import Notification


class Backpressure(Notification):
    def __init__(self, number_of_items):
        """Constructs a notification of a new value."""

        super().__init__()
        self.number_of_items = number_of_items
        self.has_value = True
        self.kind = 'B'

    def _accept(self, on_next, on_error=None, on_completed=None):
        return on_next(self.value)

    def _accept_observable(self, observer):
        return observer.on_next(self.value)

    def __str__(self):
        return "OnNext(%s)" % str(self.value)