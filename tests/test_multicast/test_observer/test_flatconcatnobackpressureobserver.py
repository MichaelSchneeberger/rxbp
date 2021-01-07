import unittest

from rx.disposable import CompositeDisposable

from rxbp.acknowledgement.continueack import ContinueAck
from rxbp.init.initobserverinfo import init_observer_info
from rxbp.multicast.observer.flatconcatnobackpressureobserver import FlatConcatNoBackpressureObserver
from rxbp.observerinfo import ObserverInfo
from rxbp.testing.tobservable import TObservable
from rxbp.testing.tobserver import TObserver
from rxbp.testing.tscheduler import TScheduler


class TestFlatConcatNoBackpressureObserver(unittest.TestCase):
    def setUp(self) -> None:
        self.scheduler = TScheduler()
        self.source = TObservable()

        self.source1 = TObservable()
        self.source2 = TObservable()
        self.source3 = TObservable()

        self.composite_disposable = CompositeDisposable()

    def test_initialize(self):
        sink = TObserver()
        observer = FlatConcatNoBackpressureObserver(
            next_observer=sink,
            selector=lambda v: v,
            scheduler=self.scheduler,
            subscribe_scheduler=self.scheduler,
            composite_disposable=self.composite_disposable,
        )
        self.source.observe(init_observer_info(observer=observer))

    def test_on_next_does_not_backpressure(self):
        sink = TObserver()
        observer = FlatConcatNoBackpressureObserver(
            next_observer=sink,
            selector=lambda v: v,
            scheduler=self.scheduler,
            subscribe_scheduler=self.scheduler,
            composite_disposable=self.composite_disposable,
        )
        self.source.observe(init_observer_info(observer=observer))

        ack1 = self.source.on_next_single(self.source1)
        ack2 = self.source.on_next_single(self.source2)

        self.assertIsInstance(ack1, ContinueAck)
        self.assertIsInstance(ack2, ContinueAck)

    def test_inner_on_next(self):
        sink = TObserver()
        observer = FlatConcatNoBackpressureObserver(
            next_observer=sink,
            selector=lambda v: v,
            scheduler=self.scheduler,
            subscribe_scheduler=self.scheduler,
            composite_disposable=self.composite_disposable,
        )
        self.source.observe(init_observer_info(observer=observer))
        self.source.on_next_single(self.source1)
        self.scheduler.advance_by(1)

        self.source1.on_next_single(1)

        self.assertEqual([1], sink.received)

    def test_on_next_on_second_source(self):
        sink = TObserver()
        observer = FlatConcatNoBackpressureObserver(
            next_observer=sink,
            selector=lambda v: v,
            scheduler=self.scheduler,
            subscribe_scheduler=self.scheduler,
            composite_disposable=self.composite_disposable,
        )
        self.source.observe(init_observer_info(observer=observer))
        self.source.on_next_single(self.source1)
        self.scheduler.advance_by(1)
        self.source1.on_next_single(1)
        self.source.on_next_single(self.source2)
        self.scheduler.advance_by(1)

        self.source2.on_next_single('a')

        self.assertEqual([1], sink.received)

    def test_complete_first_source(self):
        sink = TObserver()
        observer = FlatConcatNoBackpressureObserver(
            next_observer=sink,
            selector=lambda v: v,
            scheduler=self.scheduler,
            subscribe_scheduler=self.scheduler,
            composite_disposable=self.composite_disposable,
        )
        self.source.observe(init_observer_info(observer=observer))
        self.source.on_next_single(self.source1)
        self.scheduler.advance_by(1)
        self.source1.on_next_single(1)
        self.source.on_next_single(self.source2)
        self.scheduler.advance_by(1)
        self.source.on_completed()
        self.source2.on_next_single('a')

        self.source1.on_completed()

        self.assertEqual([1, 'a'], sink.received)
        self.assertFalse(sink.is_completed)

    def test_complete_first_source2(self):
        sink = TObserver()
        observer = FlatConcatNoBackpressureObserver(
            next_observer=sink,
            selector=lambda v: v,
            scheduler=self.scheduler,
            subscribe_scheduler=self.scheduler,
            composite_disposable=self.composite_disposable,
        )
        self.source.observe(init_observer_info(observer=observer))
        self.source.on_next_single(self.source1)
        self.scheduler.advance_by(1)
        self.source1.on_next_single(1)
        self.source.on_next_single(self.source2)
        self.scheduler.advance_by(1)
        self.source.on_completed()
        self.source2.on_next_single('a')
        self.source1.on_next_single(2)
        self.source1.on_completed()
        self.source2.on_next_single('b')

        self.source2.on_completed()

        self.assertEqual([1, 2, 'a', 'b'], sink.received)
        self.assertTrue(sink.is_completed)

    def test_three_sources(self):
        sink = TObserver()
        observer = FlatConcatNoBackpressureObserver(
            next_observer=sink,
            selector=lambda v: v,
            scheduler=self.scheduler,
            subscribe_scheduler=self.scheduler,
            composite_disposable=self.composite_disposable,
        )
        self.source.observe(init_observer_info(observer=observer))
        self.source.on_next_single(self.source1)
        self.source.on_next_single(self.source2)
        self.source.on_next_single(self.source3)
        self.scheduler.advance_by(1)

        self.source1.on_next_single(1)
        self.source2.on_next_single(2)
        self.source3.on_next_single(3)
        self.source1.on_completed()
        self.source2.on_completed()
        self.source3.on_completed()

        self.assertEqual([1, 2, 3], sink.received)