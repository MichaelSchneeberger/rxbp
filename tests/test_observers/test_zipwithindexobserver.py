import unittest

from rxbp.acknowledgement.continueack import ContinueAck
from rxbp.init.initobserverinfo import init_observer_info
from rxbp.observers.zipwithindexobserver import ZipCountObserver
from rxbp.testing.tobservable import TObservable
from rxbp.testing.tobserver import TObserver
from rxbp.testing.tscheduler import TScheduler


class TestZipWithIndexObserver(unittest.TestCase):
    def setUp(self):
        self.scheduler = TScheduler()
        self.source = TObservable()
        self.exc = Exception()

    def test_initialize(self):
        sink = TObserver()
        ZipCountObserver(
            observer=sink,
            selector=lambda v, idx: (v, idx),
        )

    def test_on_complete(self):
        sink = TObserver()
        observer = ZipCountObserver(
            observer=sink,
            selector=lambda v, idx: (v, idx),
        )
        self.source.observe(init_observer_info(observer))

        self.source.on_completed()

        self.assertTrue(sink.is_completed)

    def test_on_error(self):
        sink = TObserver()
        observer = ZipCountObserver(
            observer=sink,
            selector=lambda v, idx: (v, idx),
        )
        self.source.observe(init_observer_info(observer))

        self.source.on_error(self.exc)

        self.assertEqual(self.exc, sink.exception)

    def test_single_element(self):
        sink = TObserver()
        observer = ZipCountObserver(
            observer=sink,
            selector=lambda v, idx: (v, idx),
        )
        self.source.observe(init_observer_info(observer))

        ack = self.source.on_next_single(0)

        self.assertEqual([(0, 0)], sink.received)
        self.assertIsInstance(ack, ContinueAck)
        self.assertFalse(sink.is_completed)

        self.source.on_completed()

        self.assertTrue(sink.is_completed)

    def test_multiple_element(self):
        sink = TObserver()
        observer = ZipCountObserver(
            observer=sink,
            selector=lambda v, idx: (v, idx),
        )
        self.source.observe(init_observer_info(observer))

        self.source.on_next_single(0)
        ack = self.source.on_next_single(1)

        self.assertEqual([(0, 0), (1, 1)], sink.received)
        self.assertIsInstance(ack, ContinueAck)
        self.assertFalse(sink.is_completed)

        self.source.on_completed()

        self.assertTrue(sink.is_completed)

    def test_single_batch(self):
        sink = TObserver()
        observer = ZipCountObserver(
            observer=sink,
            selector=lambda v, idx: (v, idx),
        )
        self.source.observe(init_observer_info(observer))

        ack = self.source.on_next_list([0, 1, 2, 3])

        self.assertEqual([(0, 0), (1, 1), (2, 2), (3, 3)], sink.received)
        self.assertIsInstance(ack, ContinueAck)
        self.assertFalse(sink.is_completed)

        self.source.on_completed()

        self.assertTrue(sink.is_completed)
