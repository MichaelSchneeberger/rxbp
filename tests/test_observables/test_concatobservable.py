import unittest

from rxbp.ack.stopack import StopAck
from rxbp.ack.continueack import ContinueAck
from rxbp.observables.concatobservable import ConcatObservable
from rxbp.observerinfo import ObserverInfo
from rxbp.testing.testobservable import TestObservable
from rxbp.testing.testobserver import TestObserver
from rxbp.testing.testscheduler import TestScheduler


class TestConcatObservable(unittest.TestCase):
    def setUp(self) -> None:
        self.scheduler = TestScheduler()
        self.sources = [TestObservable(), TestObservable()]
        self.obs = ConcatObservable(
            sources=self.sources, 
            scheduler=self.scheduler, 
            subscribe_scheduler=self.scheduler,
        )
        self.exception = Exception('dummy')

    def test_initialization(self):
        sources = []
        ConcatObservable(sources=sources, scheduler=self.scheduler, subscribe_scheduler=self.scheduler)

    def test_on_next_first(self):
        sink = TestObserver()
        self.obs.observe(ObserverInfo(sink))

        ack = self.sources[0].on_next_single(1)

        self.assertIsInstance(ack, ContinueAck)
        self.assertEqual([1], sink.received)

    def test_on_next_second(self):
        sink = TestObserver()
        self.obs.observe(ObserverInfo(sink))

        ack = self.sources[1].on_next_single(1)

        self.assertFalse(ack.is_sync)
        self.assertEqual([], sink.received)

    def test_on_next_second_complete_first(self):
        sink = TestObserver()
        self.obs.observe(ObserverInfo(sink))
        ack = self.sources[1].on_next_single(1)

        self.sources[0].on_completed()

        self.assertIsInstance(ack.value, ContinueAck)
        self.assertEqual([1], sink.received)

    def test_on_error(self):
        sink = TestObserver()
        self.obs.observe(ObserverInfo(sink))

        self.sources[0].on_error(self.exception)

        self.assertEqual(self.exception, sink.exception)

    def test_on_error_on_next(self):
        sink = TestObserver()
        self.obs.observe(ObserverInfo(sink))
        self.sources[0].on_error(self.exception)

        ack = self.sources[1].on_next_single(0)

        self.assertIsInstance(ack, StopAck)
