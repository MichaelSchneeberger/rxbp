import unittest

from rxbp.ack.continueack import ContinueAck, continue_ack
from rxbp.observerinfo import ObserverInfo
from rxbp.observers.backpressurebufferedobserver import BackpressureBufferedObserver
from rxbp.testing.testobservable import TestObservable
from rxbp.testing.testobserver import TestObserver
from rxbp.testing.testscheduler import TestScheduler


class TestBackpressureBufferedObserver(unittest.TestCase):
    def setUp(self) -> None:
        self.source = TestObservable()
        self.scheduler = TestScheduler()

    def test_initialize(self):
        sink = TestObserver()
        BackpressureBufferedObserver(
            underlying=sink,
            scheduler=self.scheduler,
            subscribe_scheduler=self.scheduler,
            buffer_size=0
        )

    def test_observe(self):
        sink = TestObserver()
        observer = BackpressureBufferedObserver(
            underlying=sink,
            scheduler=self.scheduler,
            subscribe_scheduler=self.scheduler,
            buffer_size=1,
        )

        self.source.observe(ObserverInfo(observer))

    def test_on_next_zero_buffer(self):
        sink = TestObserver()
        observer = BackpressureBufferedObserver(
            underlying=sink,
            scheduler=self.scheduler,
            subscribe_scheduler=self.scheduler,
            buffer_size=0,
        )
        self.source.observe(ObserverInfo(observer))

        ack = self.source.on_next_single(0)
        self.scheduler.advance_by(1)

        self.assertFalse(ack.is_sync)
        self.assertEqual([0], sink.received)

    def test_acknowledge_zero_buffer(self):
        sink = TestObserver(immediate_continue=0)
        observer = BackpressureBufferedObserver(
            underlying=sink,
            scheduler=self.scheduler,
            subscribe_scheduler=self.scheduler,
            buffer_size=0,
        )
        self.source.observe(ObserverInfo(observer))
        ack = self.source.on_next_single(0)
        self.scheduler.advance_by(1)

        sink.ack.on_next(continue_ack)

        self.assertIsInstance(ack.value, ContinueAck)

    def test_on_next(self):
        sink = TestObserver()
        observer = BackpressureBufferedObserver(
            underlying=sink,
            scheduler=self.scheduler,
            subscribe_scheduler=self.scheduler,
            buffer_size=1,
        )
        self.source.observe(ObserverInfo(observer))

        ack = self.source.on_next_single(0)
        self.scheduler.advance_by(1)

        self.assertIsInstance(ack, ContinueAck)
        self.assertEqual([0], sink.received)

    def test_fill_up_buffer(self):
        sink = TestObserver()
        observer = BackpressureBufferedObserver(
            underlying=sink,
            scheduler=self.scheduler,
            subscribe_scheduler=self.scheduler,
            buffer_size=1,
        )
        self.source.observe(ObserverInfo(observer))
        self.source.on_next_single(0)

        ack = self.source.on_next_single(1)
        self.scheduler.advance_by(1)

        self.assertFalse(ack.is_sync)
        self.assertEqual([0, 1], sink.received)
