import unittest

from rxbp.acknowledgement.continueack import ContinueAck, continue_ack
from rxbp.init.initobserverinfo import init_observer_info
from rxbp.observerinfo import ObserverInfo
from rxbp.observers.bufferedobserver import BufferedObserver
from rxbp.testing.tobservable import TObservable
from rxbp.testing.tobserver import TObserver
from rxbp.testing.tscheduler import TScheduler


class TestBackpressureBufferedObserver(unittest.TestCase):
    def setUp(self) -> None:
        self.source = TObservable()
        self.scheduler = TScheduler()

    def test_initialize(self):
        sink = TObserver()
        BufferedObserver(
            underlying=sink,
            scheduler=self.scheduler,
            subscribe_scheduler=self.scheduler,
            buffer_size=0
        )

    def test_observe(self):
        sink = TObserver()
        observer = BufferedObserver(
            underlying=sink,
            scheduler=self.scheduler,
            subscribe_scheduler=self.scheduler,
            buffer_size=1,
        )

        self.source.observe(init_observer_info(observer))

    def test_on_next_zero_buffer(self):
        sink = TObserver()
        observer = BufferedObserver(
            underlying=sink,
            scheduler=self.scheduler,
            subscribe_scheduler=self.scheduler,
            buffer_size=0,
        )
        self.source.observe(init_observer_info(observer))

        ack = self.source.on_next_single(0)
        self.scheduler.advance_by(1)

        self.assertFalse(ack.is_sync)
        self.assertEqual([0], sink.received)

    def test_acknowledge_zero_buffer(self):
        sink = TObserver(immediate_continue=0)
        observer = BufferedObserver(
            underlying=sink,
            scheduler=self.scheduler,
            subscribe_scheduler=self.scheduler,
            buffer_size=0,
        )
        self.source.observe(init_observer_info(observer))
        ack = self.source.on_next_single(0)
        self.scheduler.advance_by(1)

        sink.ack.on_next(continue_ack)

        self.assertIsInstance(ack.value, ContinueAck)

    def test_on_next(self):
        sink = TObserver()
        observer = BufferedObserver(
            underlying=sink,
            scheduler=self.scheduler,
            subscribe_scheduler=self.scheduler,
            buffer_size=1,
        )
        self.source.observe(init_observer_info(observer))

        ack = self.source.on_next_single(0)
        self.scheduler.advance_by(1)

        self.assertIsInstance(ack, ContinueAck)
        self.assertEqual([0], sink.received)

    def test_fill_up_buffer(self):
        sink = TObserver()
        observer = BufferedObserver(
            underlying=sink,
            scheduler=self.scheduler,
            subscribe_scheduler=self.scheduler,
            buffer_size=1,
        )
        self.source.observe(init_observer_info(observer))
        self.source.on_next_single(0)

        ack = self.source.on_next_single(1)
        self.scheduler.advance_by(1)

        self.assertFalse(ack.is_sync)
        self.assertEqual([0, 1], sink.received)
