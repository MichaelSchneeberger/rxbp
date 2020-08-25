import unittest

from rx.internal import SequenceContainsNoElementsError

from rxbp.acknowledgement.stopack import StopAck
from rxbp.init.initobserverinfo import init_observer_info
from rxbp.observers.defaultifemptyobserver import DefaultIfEmptyObserver
from rxbp.observers.filterobserver import FilterObserver
from rxbp.observers.firstobserver import FirstObserver
from rxbp.testing.testobservable import TestObservable
from rxbp.testing.testobserver import TestObserver
from rxbp.testing.testscheduler import TestScheduler


class TestDefaultIfEmptyObserver(unittest.TestCase):
    def setUp(self):
        self.scheduler = TestScheduler()
        self.source = TestObservable()
        self.exc = Exception()

    def test_initialize(self):
        sink = TestObserver()
        DefaultIfEmptyObserver(
            observer=sink,
            lazy_val=lambda: 0,
        )

    def test_on_complete(self):
        sink = TestObserver()
        observer = DefaultIfEmptyObserver(
            observer=sink,
            lazy_val=lambda: 0,
        )
        self.source.observe(init_observer_info(observer))

        self.source.on_completed()

        self.assertEqual([0], sink.received)
        self.assertTrue(sink.is_completed)

    def test_on_error(self):
        sink = TestObserver()
        observer = DefaultIfEmptyObserver(
            observer=sink,
            lazy_val=lambda: 0,
        )
        self.source.observe(init_observer_info(observer))

        self.source.on_error(self.exc)

        self.assertEqual(self.exc, sink.exception)

    def test_single_element(self):
        sink = TestObserver()
        observer = DefaultIfEmptyObserver(
            observer=sink,
            lazy_val=lambda: 0,
        )
        self.source.observe(init_observer_info(observer))

        ack = self.source.on_next_single(1)

        self.assertEqual([1], sink.received)
        self.assertFalse(sink.is_completed)

        self.source.on_completed()

        self.assertTrue(sink.is_completed)

    def test_single_batch(self):
        sink = TestObserver()
        observer = DefaultIfEmptyObserver(
            observer=sink,
            lazy_val=lambda: 0,
        )
        self.source.observe(init_observer_info(observer))

        ack = self.source.on_next_list([1, 2, 3])

        self.assertEqual([1, 2, 3], sink.received)
