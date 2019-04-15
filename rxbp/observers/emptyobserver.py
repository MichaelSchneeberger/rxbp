from rxbp.ack import continue_ack
from rxbp.observer import Observer


class EmptyObserver(Observer):
    def on_next(self, _):
        return continue_ack

    def on_error(self, _):
        pass

    def on_completed(self):
        pass
