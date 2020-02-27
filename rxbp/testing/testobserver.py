from rxbp.ack.acksubject import AckSubject
from rxbp.ack.continueack import continue_ack
from rxbp.observer import Observer
from rxbp.typing import ElementType


class TestObserver(Observer):
    """ A test observer that immediately returns a Continue acknowledgment for some number of times, otherwise it
    returns an asynchronous acknowledgment
    """

    def __init__(self, immediate_continue: int = None):
        self.received = []
        self.is_completed = False
        self.exception = None
        self.immediate_continue = immediate_continue
        self.ack = None

        # counts the number of times `on_next` is called
        self.on_next_counter = 0

    def on_next(self, elem: ElementType):
        self.on_next_counter += 1
        values = list(elem)
        self.received += values
        if self.immediate_continue is None:
            return continue_ack
        elif 0 < self.immediate_continue:
            self.immediate_continue -= 1
            return continue_ack
        else:
            self.ack = AckSubject()
            return self.ack

    def on_error(self, err):
        self.exception = err

    def on_completed(self):
        self.is_completed = True
