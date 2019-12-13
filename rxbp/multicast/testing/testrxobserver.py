from rx.core.abc import Observer


class TestRxObserver(Observer):
    def __init__(self):
        self.received = []
        self.is_completed = False
        self.exception = None

    def on_next(self, val):
        self.received.append(val)

    def on_error(self, error):
        self.exception = error

    def on_completed(self):
        self.is_completed = True
