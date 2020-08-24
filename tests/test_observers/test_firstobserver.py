import unittest

from rx.internal import SequenceContainsNoElementsError

from rxbp.ack.stopack import StopAck
from rxbp.init.initobserverinfo import init_observer_info
from rxbp.observers.filterobserver import FilterObserver
from rxbp.observers.firstobserver import FirstObserver
from rxbp.testing.testobservable import TestObservable
from rxbp.testing.testobserver import TestObserver
from rxbp.testing.testscheduler import TestScheduler
from rxbp.utils.getstacklines import get_stack_lines


class TestFirstObserver(unittest.TestCase):
    def setUp(self):
        self.scheduler = TestScheduler()
        self.source = TestObservable()
        self.exc = Exception()

    def test_initialize(self):
        sink = TestObserver()
        FirstObserver(
            observer=sink,
            stack=get_stack_lines(),
        )

    def test_on_complete(self):
        sink = TestObserver()
        observer = FirstObserver(
            observer=sink,
            stack=get_stack_lines(),
        )
        self.source.observe(init_observer_info(observer))

        self.source.on_completed()

        self.assertFalse(sink.is_completed)
        self.assertIsInstance(sink.exception, SequenceContainsNoElementsError)

    def test_on_error(self):
        sink = TestObserver()
        observer = FirstObserver(
            observer=sink,
            stack=get_stack_lines(),
        )
        self.source.observe(init_observer_info(observer))

        self.source.on_error(self.exc)

        self.assertEqual(self.exc, sink.exception)

    def test_single_element(self):
        sink = TestObserver()
        observer = FirstObserver(
            observer=sink,
            stack=get_stack_lines(),
        )
        self.source.observe(init_observer_info(observer))

        ack = self.source.on_next_single(0)

        self.assertEqual([0], sink.received)
        self.assertIsInstance(ack, StopAck)
        self.assertTrue(sink.is_completed)

        self.source.on_completed()

        self.assertEqual([0], sink.received)
        self.assertTrue(sink.is_completed)

    def test_single_batch(self):
        sink = TestObserver()
        observer = FirstObserver(
            observer=sink,
            stack=get_stack_lines(),
        )
        self.source.observe(init_observer_info(observer))

        ack = self.source.on_next_list([0, 1, 2, 3])

        self.assertEqual([0], sink.received)
        self.assertIsInstance(ack, StopAck)
        self.assertTrue(sink.is_completed)
