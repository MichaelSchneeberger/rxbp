from rx.core.notification import Notification


class BPResponse(Notification):
    def __init__(self, number_of_items):
        """Constructs a notification of a new value."""

        super().__init__()
        self.number_of_items = number_of_items
        self.has_value = True
        self.kind = 'B'

    def _accept(self, on_next, on_error=None, on_completed=None):
        return on_next(self.number_of_items)

    def _accept_observable(self, observer):
        return observer.on_next(self.number_of_items)

    def __str__(self):
        return "BPResponse(%s)" % str(self.number_of_items)