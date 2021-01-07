import unittest

from rxbp.acknowledgement.stopack import StopAck
from rxbp.acknowledgement.continueack import ContinueAck
from rxbp.init.initobserverinfo import init_observer_info
from rxbp.observables.concatobservable import ConcatObservable
from rxbp.observerinfo import ObserverInfo
from rxbp.testing.tobservable import TObservable
from rxbp.testing.tobserver import TObserver
from rxbp.testing.tscheduler import TScheduler


class TestConcatObservable(unittest.TestCase):
    def setUp(self) -> None:
        self.scheduler = TScheduler()
        self.sources = [TObservable(), TObservable()]
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
        sink = TObserver()
        self.obs.observe(init_observer_info(sink))

        ack = self.sources[0].on_next_single(1)

        self.assertIsInstance(ack, ContinueAck)
        self.assertEqual([1], sink.received)

    def test_on_next_second(self):
        sink = TObserver()
        self.obs.observe(init_observer_info(sink))

        ack = self.sources[1].on_next_single(1)

        self.assertFalse(ack.is_sync)
        self.assertEqual([], sink.received)

    def test_on_next_second_complete_first(self):
        sink = TObserver()
        self.obs.observe(init_observer_info(sink))
        ack = self.sources[1].on_next_single(1)

        self.sources[0].on_completed()

        self.assertIsInstance(ack.value, ContinueAck)
        self.assertEqual([1], sink.received)

    def test_on_error(self):
        sink = TObserver()
        self.obs.observe(init_observer_info(sink))

        self.sources[0].on_error(self.exception)

        self.assertEqual(self.exception, sink.exception)

    def test_on_error_on_next(self):
        sink = TObserver()
        self.obs.observe(init_observer_info(sink))
        self.sources[0].on_error(self.exception)

        ack = self.sources[1].on_next_single(0)

        self.assertIsInstance(ack, StopAck)
