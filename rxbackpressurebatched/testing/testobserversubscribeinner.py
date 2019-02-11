from rxbackpressurebatched.ack import Ack
from rxbackpressurebatched.observer import Observer
from rxbackpressurebatched.schedulers.currentthreadscheduler import CurrentThreadScheduler
from rxbackpressurebatched.testing.testobserver import TestObserver


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
        self.received.append(v)
        self.ack = Ack()
        self.inner_obs = TestObserver()
        self.inner_selector(v).subscribe(self.inner_obs, self.scheduler, CurrentThreadScheduler())
        return self.ack

    def on_error(self, err):
        pass

    def on_completed(self):
        self.is_completed = True