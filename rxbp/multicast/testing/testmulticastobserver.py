from rxbp.multicast.multicastobserver import MultiCastObserver


class TestMultiCastObserver(MultiCastObserver):
    def __init__(self):
        self.received = []
        self.is_completed = False
        self.exception = None

    def on_next(self, val):
        self.received.extend(val)

    def on_error(self, error):
        self.exception = error

    def on_completed(self):
        self.is_completed = True
