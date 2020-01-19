from typing import List, Callable, Any

from rxbp.ack.acksubject import AckSubject
from rxbp.ack.continueack import continue_ack
from rxbp.observer import Observer
from rxbp.scheduler import Scheduler
from rxbp.testing.testobserver import TestObserver
from rxbp.typing import ValueType


class TestObserverSubscribeInner(Observer):
    """ The test observer subscribes a new inner observer for each received element. A selector selects the
    observable from the received element
    """

    def __init__(
            self,
            inner_selector: Callable[[ValueType], Any],
            scheduler: Scheduler,
            immediate_continue: bool = None,
            inner_immediate_continue: bool = None,
    ):
        self.received = []
        self.is_completed = False
        self.inner_selector = inner_selector
        self.inner_obs: List[TestObserver] = []
        self.ack = None
        self.scheduler = scheduler
        self.immediate_continue = immediate_continue
        self.inner_immediate_continue = inner_immediate_continue

    def on_next(self, v):
        values = list(v())
        self.received.append(values)
        for value in values:
            observer = TestObserver(immediate_coninue=self.inner_immediate_continue)
            self.inner_obs.append(observer)
            self.inner_selector(value).observe(observer)

        if self.immediate_continue is None or 0 < self.immediate_continue:
            self.immediate_continue -= 1
            return continue_ack
        else:
            self.ack = AckSubject()
            return self.ack

    def on_error(self, err):
        pass

    def on_completed(self):
        self.is_completed = True
