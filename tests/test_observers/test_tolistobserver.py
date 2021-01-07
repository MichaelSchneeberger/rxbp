import unittest

from rx.internal import SequenceContainsNoElementsError

from rxbp.acknowledgement.continueack import ContinueAck
from rxbp.acknowledgement.stopack import StopAck
from rxbp.init.initobserverinfo import init_observer_info
from rxbp.observers.filterobserver import FilterObserver
from rxbp.observers.firstobserver import FirstObserver
from rxbp.observers.tolistobserver import ToListObserver
from rxbp.testing.tobservable import TObservable
from rxbp.testing.tobserver import TObserver
from rxbp.testing.tscheduler import TScheduler


class TestToListObserver(unittest.TestCase):
    def setUp(self):
        self.scheduler = TScheduler()
        self.source = TObservable()
        self.exc = Exception()

    def test_initialize(self):
        sink = TObserver()
        ToListObserver(
            observer=sink,
        )

    def test_on_complete(self):
        sink = TObserver()
        observer = ToListObserver(
            observer=sink,
        )
        self.source.observe(init_observer_info(observer))

        self.source.on_completed()

        self.assertEqual([[]], sink.received)
        self.assertTrue(sink.is_completed)

    def test_on_error(self):
        sink = TObserver()
        observer = ToListObserver(
            observer=sink,
        )
        self.source.observe(init_observer_info(observer))

        self.source.on_error(self.exc)

        self.assertEqual(self.exc, sink.exception)

    def test_single_element(self):
        sink = TObserver()
        observer = ToListObserver(
            observer=sink,
        )
        self.source.observe(init_observer_info(observer))

        ack = self.source.on_next_single(0)

        self.assertEqual([], sink.received)
        self.assertIsInstance(ack, ContinueAck)
        self.assertFalse(sink.is_completed)

        self.source.on_completed()

        self.assertEqual([[0]], sink.received)
        self.assertTrue(sink.is_completed)

    def test_single_batch(self):
        sink = TObserver()
        observer = ToListObserver(
            observer=sink,
        )
        self.source.observe(init_observer_info(observer))

        ack = self.source.on_next_list([0, 1, 2, 3])

        self.assertEqual([], sink.received)
        self.assertIsInstance(ack, ContinueAck)
        self.assertFalse(sink.is_completed)

        self.source.on_completed()

        self.assertEqual([[0, 1, 2, 3]], sink.received)
        self.assertTrue(sink.is_completed)
