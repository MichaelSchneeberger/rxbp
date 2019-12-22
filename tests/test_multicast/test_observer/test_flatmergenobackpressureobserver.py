import unittest

from rxbp.ack.continueack import ContinueAck
from rxbp.multicast.observer.flatconcatnobackpressureobserver import FlatConcatNoBackpressureObserver
from rxbp.multicast.observer.flatmergenobackpressureobserver import FlatMergeNoBackpressureObserver
from rxbp.observerinfo import ObserverInfo
from rxbp.testing.testobservable import TestObservable
from rxbp.testing.testobserver import TestObserver
from rxbp.testing.testscheduler import TestScheduler


class TestFlatMergeNoBackpressureObserver(unittest.TestCase):
    def setUp(self) -> None:
        self.scheduler = TestScheduler()
        self.source = TestObservable()

        self.source1 = TestObservable()
        self.source2 = TestObservable()
        self.source3 = TestObservable()

    def test_initialize(self):
        sink = TestObserver()
        observer = FlatMergeNoBackpressureObserver(
            observer=sink,
            selector=lambda v: v,
            scheduler=self.scheduler,
            subscribe_scheduler=self.scheduler,
            is_volatile=False,
        )
        self.source.observe(ObserverInfo(observer=observer))

    def test_on_next_does_not_backpressure(self):
        sink = TestObserver()
        observer = FlatMergeNoBackpressureObserver(
            observer=sink,
            selector=lambda v: v,
            scheduler=self.scheduler,
            subscribe_scheduler=self.scheduler,
            is_volatile=False,
        )
        self.source.observe(ObserverInfo(observer=observer))

        ack1 = self.source.on_next_single(self.source1)
        ack2 = self.source.on_next_single(self.source2)

        self.assertIsInstance(ack1, ContinueAck)
        self.assertIsInstance(ack2, ContinueAck)

    def test_inner_on_next(self):
        sink = TestObserver()
        observer = FlatMergeNoBackpressureObserver(
            observer=sink,
            selector=lambda v: v,
            scheduler=self.scheduler,
            subscribe_scheduler=self.scheduler,
            is_volatile=False,
        )
        self.source.observe(ObserverInfo(observer=observer))
        self.source.on_next_single(self.source1)

        self.source1.on_next_single(1)

        self.assertEqual([1], sink.received)

    def test_on_next_on_second_source(self):
        sink = TestObserver()
        observer = FlatMergeNoBackpressureObserver(
            observer=sink,
            selector=lambda v: v,
            scheduler=self.scheduler,
            subscribe_scheduler=self.scheduler,
            is_volatile=False,
        )
        self.source.observe(ObserverInfo(observer=observer))
        self.source.on_next_single(self.source1)
        self.source1.on_next_single(1)
        self.source.on_next_single(self.source2)

        self.source2.on_next_single('a')

        self.assertEqual([1, 'a'], sink.received)

    def test_complete_first_source(self):
        sink = TestObserver()
        observer = FlatMergeNoBackpressureObserver(
            observer=sink,
            selector=lambda v: v,
            scheduler=self.scheduler,
            subscribe_scheduler=self.scheduler,
            is_volatile=False,
        )
        self.source.observe(ObserverInfo(observer=observer))
        self.source.on_next_single(self.source1)
        self.source1.on_next_single(1)
        self.source.on_next_single(self.source2)
        self.source.on_completed()
        self.source2.on_next_single('a')

        self.source1.on_completed()

        self.assertEqual([1, 'a'], sink.received)
        self.assertFalse(sink.is_completed)

    def test_complete_first_source2(self):
        sink = TestObserver()
        observer = FlatMergeNoBackpressureObserver(
            observer=sink,
            selector=lambda v: v,
            scheduler=self.scheduler,
            subscribe_scheduler=self.scheduler,
            is_volatile=False,
        )
        self.source.observe(ObserverInfo(observer=observer))
        self.source.on_next_single(self.source1)
        self.source1.on_next_single(1)
        self.source.on_next_single(self.source2)
        self.source.on_completed()
        self.source2.on_next_single('a')
        self.source1.on_next_single(2)
        self.source1.on_completed()
        self.source2.on_next_single('b')

        self.source2.on_completed()

        self.assertEqual([1, 'a', 2, 'b'], sink.received)
        self.assertTrue(sink.is_completed)

    def test_three_sources(self):
        sink = TestObserver()
        observer = FlatMergeNoBackpressureObserver(
            observer=sink,
            selector=lambda v: v,
            scheduler=self.scheduler,
            subscribe_scheduler=self.scheduler,
            is_volatile=False,
        )
        self.source.observe(ObserverInfo(observer=observer))
        self.source.on_next_single(self.source1)
        self.source.on_next_single(self.source2)
        self.source.on_next_single(self.source3)

        self.source1.on_next_single(1)
        self.source2.on_next_single(2)
        self.source3.on_next_single(3)

        self.assertEqual([1, 2, 3], sink.received)