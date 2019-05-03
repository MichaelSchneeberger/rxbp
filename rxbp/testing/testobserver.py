from rxbp.ack import Continue, Ack
from rxbp.observer import Observer


class TestObserver(Observer):
    """ A test observer that immediately returns a Continue acknowledgment for some number of times, otherwise it
    returns an asynchroneous acknowledgment
    """

    def __init__(self):
        self.received = []
        self.is_completed = False
        self.was_thrown = None
        self.immediate_continue = 0
        self.ack = None

    def on_next(self, v):
        values = list(v())
        self.received += values
        if 0 < self.immediate_continue:
            self.immediate_continue -= 1
            return Continue()
        else:
            self.ack = Ack()
            return self.ack

    def on_error(self, err):
        self.was_thrown = err

    def on_completed(self):
        self.is_completed = True
