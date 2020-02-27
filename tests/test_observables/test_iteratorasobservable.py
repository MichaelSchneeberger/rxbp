import unittest

from rxbp.ack.continueack import continue_ack
from rxbp.observables.iteratorasobservable import IteratorAsObservable
from rxbp.observerinfo import ObserverInfo
from rxbp.testing.testobserver import TestObserver
from rxbp.testing.testscheduler import TestScheduler


class TestIteratorAsObservable(unittest.TestCase):
    def setUp(self) -> None:
        self.scheduler = TestScheduler()

    def test_initialize(self):
        IteratorAsObservable(
            iterator=iter([]),
            scheduler=self.scheduler,
            subscribe_scheduler=self.scheduler
        )

    def test_empty_iterable(self):
        obs = IteratorAsObservable(
            iterator=iter([]),
            scheduler=self.scheduler,
            subscribe_scheduler=self.scheduler
        )
        sink = TestObserver()

        obs.observe(ObserverInfo(sink))
        self.scheduler.advance_by(1)

        self.assertEqual([], sink.received)
        self.assertTrue(sink.is_completed)

    def test_single_elem(self):
        obs = IteratorAsObservable(
            iterator=iter([[1]]),
            scheduler=self.scheduler,
            subscribe_scheduler=self.scheduler
        )
        sink = TestObserver(immediate_continue=0)
        obs.observe(ObserverInfo(sink))
        self.scheduler.advance_by(1)

        self.assertEqual([1], sink.received)
        self.assertTrue(sink.is_completed)

    def test_batch(self):
        obs = IteratorAsObservable(
            iterator=iter([[1, 2, 3]]),
            scheduler=self.scheduler,
            subscribe_scheduler=self.scheduler
        )
        sink = TestObserver(immediate_continue=0)
        obs.observe(ObserverInfo(sink))
        self.scheduler.advance_by(1)

        self.assertEqual([1, 2, 3], sink.received)
        self.assertTrue(sink.is_completed)

    def test_multiple_elem_sync_ack(self):
        obs = IteratorAsObservable(
            iterator=iter([[1], [2, 3], [4]]),
            scheduler=self.scheduler,
            subscribe_scheduler=self.scheduler
        )
        sink = TestObserver()
        obs.observe(ObserverInfo(sink))
        self.scheduler.advance_by(1)

        self.assertEqual([1, 2, 3, 4], sink.received)
        self.assertTrue(sink.is_completed)

    def test_multiple_elem_async_ack(self):
        obs = IteratorAsObservable(
            iterator=iter([[1], [2]]),
            scheduler=self.scheduler,
            subscribe_scheduler=self.scheduler
        )
        sink = TestObserver(immediate_continue=0)
        obs.observe(ObserverInfo(sink))
        self.scheduler.advance_by(1)

        self.assertEqual([1], sink.received)
        self.assertFalse(sink.is_completed)

    def test_multiple_elem_async_ack_2nd_part(self):
        obs = IteratorAsObservable(
            iterator=iter([[1], [2]]),
            scheduler=self.scheduler,
            subscribe_scheduler=self.scheduler
        )
        sink = TestObserver(immediate_continue=0)
        obs.observe(ObserverInfo(sink))
        self.scheduler.advance_by(1)

        sink.ack.on_next(continue_ack)
        self.scheduler.advance_by(1)

        self.assertEqual([1, 2], sink.received)
        self.assertTrue(sink.is_completed)