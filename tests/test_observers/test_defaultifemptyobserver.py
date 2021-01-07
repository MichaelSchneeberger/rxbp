import unittest

from rx.internal import SequenceContainsNoElementsError

from rxbp.acknowledgement.stopack import StopAck
from rxbp.init.initobserverinfo import init_observer_info
from rxbp.observers.defaultifemptyobserver import DefaultIfEmptyObserver
from rxbp.observers.filterobserver import FilterObserver
from rxbp.observers.firstobserver import FirstObserver
from rxbp.testing.tobservable import TObservable
from rxbp.testing.tobserver import TObserver
from rxbp.testing.tscheduler import TScheduler


class TestDefaultIfEmptyObserver(unittest.TestCase):
    def setUp(self):
        self.scheduler = TScheduler()
        self.source = TObservable()
        self.exc = Exception()

    def test_initialize(self):
        sink = TObserver()
        DefaultIfEmptyObserver(
            next_observer=sink,
            lazy_val=lambda: 0,
        )

    def test_on_complete(self):
        sink = TObserver()
        observer = DefaultIfEmptyObserver(
            next_observer=sink,
            lazy_val=lambda: 0,
        )
        self.source.observe(init_observer_info(observer))

        self.source.on_completed()

        self.assertEqual([0], sink.received)
        self.assertTrue(sink.is_completed)

    def test_on_error(self):
        sink = TObserver()
        observer = DefaultIfEmptyObserver(
            next_observer=sink,
            lazy_val=lambda: 0,
        )
        self.source.observe(init_observer_info(observer))

        self.source.on_error(self.exc)

        self.assertEqual(self.exc, sink.exception)

    def test_single_element(self):
        sink = TObserver()
        observer = DefaultIfEmptyObserver(
            next_observer=sink,
            lazy_val=lambda: 0,
        )
        self.source.observe(init_observer_info(observer))

        ack = self.source.on_next_single(1)

        self.assertEqual([1], sink.received)
        self.assertFalse(sink.is_completed)

        self.source.on_completed()

        self.assertTrue(sink.is_completed)

    def test_single_batch(self):
        sink = TObserver()
        observer = DefaultIfEmptyObserver(
            next_observer=sink,
            lazy_val=lambda: 0,
        )
        self.source.observe(init_observer_info(observer))

        ack = self.source.on_next_list([1, 2, 3])

        self.assertEqual([1, 2, 3], sink.received)
