import unittest

from rxbp.acknowledgement.continueack import ContinueAck, continue_ack
from rxbp.acknowledgement.stopack import StopAck
from rxbp.init.initobserverinfo import init_observer_info
from rxbp.observers.filterobserver import FilterObserver
from rxbp.observers.takewhileobserver import TakeWhileObserver
from rxbp.testing.tobservable import TObservable
from rxbp.testing.tobserver import TObserver
from rxbp.testing.tscheduler import TScheduler


class TestTakeWhileObserver(unittest.TestCase):
    def setUp(self):
        self.scheduler = TScheduler()
        self.source = TObservable()
        self.exception = Exception()

    def test_initialize(self):
        sink = TObserver()
        TakeWhileObserver(
            observer=sink,
            predicate=lambda _: True,
        )

    def test_empty_sequence(self):
        sink = TObserver()
        observer = TakeWhileObserver(
            observer=sink,
            predicate=lambda v: v > 0,
        )
        self.source.observe(init_observer_info(observer))

        self.source.on_completed()

        self.assertTrue(sink.is_completed)

    def test_on_error(self):
        sink = TObserver()
        observer = TakeWhileObserver(
            observer=sink,
            predicate=lambda v: v > 0,
        )
        self.source.observe(init_observer_info(observer))

        self.source.on_error(self.exception)

        self.assertEqual(self.exception, sink.exception)

    def test_single_non_matching_element_synchronously(self):
        sink = TObserver(immediate_continue=None)
        observer = TakeWhileObserver(
            observer=sink,
            predicate=lambda v: v,
        )
        self.source.observe(init_observer_info(observer))

        ack = self.source.on_next_list([0])

        self.assertIsInstance(ack, StopAck)
        self.assertTrue(sink.is_completed)
        self.assertListEqual(sink.received, [])

    def test_single_matching_elements_synchronously(self):
        sink = TObserver(immediate_continue=None)
        observer = TakeWhileObserver(
            observer=sink,
            predicate=lambda v: v,
        )
        self.source.observe(init_observer_info(observer))

        ack = self.source.on_next_list([1])
        self.assertIsInstance(ack, ContinueAck)
        self.assertListEqual(sink.received, [1])

        ack = self.source.on_next_list([1])
        self.assertIsInstance(ack, ContinueAck)
        self.assertListEqual(sink.received, [1, 1])

        ack = self.source.on_next_list([0])
        self.assertIsInstance(ack, StopAck)
        self.assertListEqual(sink.received, [1, 1])

    def test_list_and_complete_synchronously(self):
        sink = TObserver(immediate_continue=None)
        observer = TakeWhileObserver(
            observer=sink,
            predicate=lambda v: v,
        )
        self.source.observe(init_observer_info(observer))

        ack = self.source.on_next_list([1, 1, 1])
        self.assertIsInstance(ack, ContinueAck)
        self.assertEqual(1, sink.on_next_counter)
        self.assertListEqual(sink.received, [1, 1, 1])

        self.source.on_completed()
        self.assertListEqual(sink.received, [1, 1, 1])
        self.assertTrue(sink.is_completed)

    def test_list_synchronously(self):
        sink = TObserver(immediate_continue=None)
        observer = TakeWhileObserver(
            observer=sink,
            predicate=lambda v: v,
        )
        self.source.observe(init_observer_info(observer))

        ack = self.source.on_next_list([1, 1, 1])
        self.assertIsInstance(ack, ContinueAck)
        self.assertEqual(1, sink.on_next_counter)
        self.assertListEqual(sink.received, [1, 1, 1])

        ack = self.source.on_next_list([1, 0, 1])
        self.assertIsInstance(ack, StopAck)
        self.assertEqual(2, sink.on_next_counter)
        self.assertListEqual(sink.received, [1, 1, 1, 1])
        self.assertTrue(sink.is_completed)

    def test_iterable_synchronously(self):
        sink = TObserver(immediate_continue=None)
        observer = TakeWhileObserver(
            observer=sink,
            predicate=lambda v: v,
        )
        self.source.observe(init_observer_info(observer))

        ack = self.source.on_next_iter([1, 1, 1])
        self.assertIsInstance(ack, ContinueAck)
        self.assertEqual(1, sink.on_next_counter)
        self.assertListEqual(sink.received, [1, 1, 1])

        ack = self.source.on_next_iter([1, 0, 1])
        self.assertIsInstance(ack, StopAck)
        self.assertEqual(2, sink.on_next_counter)
        self.assertListEqual(sink.received, [1, 1, 1, 1])
        self.assertTrue(sink.is_completed)

    def test_failure_synchronously(self):
        sink = TObserver(immediate_continue=None)
        observer = TakeWhileObserver(
            observer=sink,
            predicate=lambda v: v,
        )
        self.source.observe(init_observer_info(observer))

        def gen_iterable():
            for i in range(10):
                if i == 3:
                    raise self.exception
                yield 1

        ack = self.source.on_next_iter(gen_iterable())

        self.assertIsInstance(ack, StopAck)
        self.assertEqual(0, sink.on_next_counter)
        self.assertListEqual([], sink.received)
        self.assertEqual(self.exception, sink.exception)

    def test_failure_after_non_matching_element_synchronously(self):
        sink = TObserver(immediate_continue=None)
        observer = TakeWhileObserver(
            observer=sink,
            predicate=lambda v: v,
        )
        self.source.observe(init_observer_info(observer))

        def gen_iterable():
            for i in range(10):
                if i == 2:
                    yield 0
                elif i == 3:
                    raise self.exception
                else:
                    yield 1

        ack = self.source.on_next_iter(gen_iterable())

        self.assertIsInstance(ack, StopAck)
        self.assertEqual(1, sink.on_next_counter)
        self.assertListEqual([1, 1], sink.received)
        self.assertTrue(sink.is_completed)
        self.assertIsNone(sink.exception)

    def test_list_asynchronously(self):
        sink = TObserver(immediate_continue=0)
        observer = TakeWhileObserver(
            observer=sink,
            predicate=lambda v: v,
        )
        self.source.observe(init_observer_info(observer))

        ack = self.source.on_next_list([1, 1, 1])

        self.assertFalse(ack.is_sync)
        self.assertEqual(1, sink.on_next_counter)
        self.assertListEqual(sink.received, [1, 1, 1])

        ack.on_next(continue_ack)

        ack = self.source.on_next_list([1, 0, 1])

        self.assertIsInstance(ack, StopAck)
        self.assertEqual(2, sink.on_next_counter)
        self.assertListEqual(sink.received, [1, 1, 1, 1])
