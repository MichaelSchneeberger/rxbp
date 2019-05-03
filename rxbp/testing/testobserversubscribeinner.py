from rxbp.ack import Ack
from rxbp.observer import Observer
from rxbp.schedulers.trampolinescheduler import TrampolineScheduler
from rxbp.testing.testobserver import TestObserver


class TestObserverSubscribeInner(Observer):
    """ The test observer subscribes a new inner observer for each received element. A selector selects the
    observable from the received element
    """

    def __init__(self, inner_selector, scheduler):
        self.received = []
        self.is_completed = False
        self.inner_selector = inner_selector
        self.inner_obs: TestObserver = None
        self.ack = None
        self.scheduler = scheduler

    def on_next(self, v):
        values = list(v())
        self.received.append(values)
        self.ack = Ack()
        self.inner_obs = TestObserver()
        for value in values:
            self.inner_selector(value).observe(self.inner_obs)
        return self.ack

    def on_error(self, err):
        pass

    def on_completed(self):
        self.is_completed = True
