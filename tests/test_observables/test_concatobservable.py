import unittest

from rxbp.ack.ackimpl import Continue, Stop
from rxbp.observables.concatobservable import ConcatObservable
from rxbp.observerinfo import ObserverInfo
from rxbp.testing.testobservable import TestObservable
from rxbp.testing.testobserver import TestObserver
from rxbp.testing.testscheduler import TestScheduler


class TestConcatObservable(unittest.TestCase):
    def setUp(self) -> None:
        self.scheduler = TestScheduler()
        self.sink = TestObserver()
        self.exception = Exception('dummy')

    def test_initialization(self):
        sources = []
        ConcatObservable(sources=sources, scheduler=self.scheduler, subscribe_scheduler=self.scheduler)

    def test_no_backpressure(self):
        sources = [TestObservable(), TestObservable()]
        obs = ConcatObservable(sources=sources, scheduler=self.scheduler, subscribe_scheduler=self.scheduler)
        obs.observe(ObserverInfo(self.sink))

        ack = sources[0].on_next_single(1)

        self.assertIsInstance(ack, Continue)

    def test_backpressure(self):
        sources = [TestObservable(), TestObservable()]
        obs = ConcatObservable(sources=sources, scheduler=self.scheduler, subscribe_scheduler=self.scheduler)
        obs.observe(ObserverInfo(self.sink))

        ack = sources[1].on_next_single(1)

        self.assertNotIsInstance(ack, Continue)

    def test_on_error(self):
        sources = [TestObservable(), TestObservable()]
        obs = ConcatObservable(sources=sources, scheduler=self.scheduler, subscribe_scheduler=self.scheduler)
        obs.observe(ObserverInfo(self.sink))

        sources[0].on_error(self.exception)

        self.assertEqual(self.exception, self.sink.exception)

    def test_on_error_on_next(self):
        sources = [TestObservable(), TestObservable()]
        obs = ConcatObservable(sources=sources, scheduler=self.scheduler, subscribe_scheduler=self.scheduler)
        obs.observe(ObserverInfo(self.sink))
        sources[0].on_error(self.exception)

        ack = sources[1].on_next_single(0)

        self.assertIsInstance(ack, Stop)
