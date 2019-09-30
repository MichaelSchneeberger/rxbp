from rxbp.ack.ack import Ack
from rxbp.ack.ackimpl import continue_ack, Continue
from rxbp.observables.mergeobservable import MergeObservable
from rxbp.observables.zip2observable import Zip2Observable
from rxbp.observerinfo import ObserverInfo
from rxbp.testing.testcasebase import TestCaseBase
from rxbp.testing.testobservable import TestObservable
from rxbp.testing.testobserver import TestObserver
from rxbp.testing.testscheduler import TestScheduler


class TestMergeObservable(TestCaseBase):
    def setUp(self):
        self.scheduler = TestScheduler()
        self.s1 = TestObservable()
        self.s2 = TestObservable()

    def test_emit_simultaneously_synchronous_ack(self):
        sink = TestObserver()
        obs = MergeObservable(self.s1, self.s2)
        obs.observe(ObserverInfo(sink))

        self.s1.on_next_single(1)
        self.assertEqual(sink.received, [1])

        self.s1.on_next_single(1)
        self.assertEqual(sink.received, [1, 1])

        self.s2.on_next_single(2)
        self.assertEqual(sink.received, [1, 1, 2])

        self.s1.on_completed()
        self.assertFalse(sink.is_completed)

        self.s2.on_next_single(2)
        self.assertEqual(sink.received, [1, 1, 2, 2])

        self.s2.on_next_single(2)
        self.assertEqual(sink.received, [1, 1, 2, 2, 2])

        self.s2.on_completed()
        self.assertTrue(sink.is_completed)

    def test_emit_consecutively_synchronous_ack(self):
        sink = TestObserver()
        obs = MergeObservable(self.s1, self.s2)
        obs.observe(ObserverInfo(sink))

        self.s1.on_next_single(1)
        self.assertEqual(sink.received, [1])

        self.s1.on_completed()
        self.assertFalse(sink.is_completed)

        self.s2.on_next_single(2)
        self.assertEqual(sink.received, [1, 2])

        self.s2.on_next_single(2)
        self.assertEqual(sink.received, [1, 2, 2])

        self.s2.on_completed()
        self.assertTrue(sink.is_completed)

    def test_emit_simultaneously_1_asynchronous_ack(self):
        sink = TestObserver(immediate_coninue=0)
        obs = MergeObservable(self.s1, self.s2)
        obs.observe(ObserverInfo(sink))

        left_ack = self.s1.on_next_single(1)
        self.assertEqual(sink.received, [1])

        self.s1.on_completed()
        self.assertFalse(sink.is_completed)

        right_ack = self.s2.on_next_single(2)
        self.assertEqual(sink.received, [1])

        self.s2.on_completed()
        self.assertFalse(sink.is_completed)

        sink.ack.on_next(continue_ack)
        self.scheduler.advance_by(1)
        self.assertEqual([1, 2], sink.received)
        self.assertTrue(sink.is_completed)

    def test_emit_simultaneously_2_asynchronous_ack(self):
        sink = TestObserver(immediate_coninue=0)
        obs = MergeObservable(self.s1, self.s2)
        obs.observe(ObserverInfo(sink))

        left_ack = self.s1.on_next_single(1)
        self.assertEqual([1], sink.received)
        self.assertIsInstance(left_ack, Continue)

        left_ack = self.s1.on_next_single(1)
        self.assertEqual([1], sink.received)

        self.s1.on_completed()
        self.assertFalse(sink.is_completed)

        right_ack = self.s2.on_next_single(2)
        self.assertEqual([1], sink.received)

        self.s2.on_completed()
        self.assertFalse(sink.is_completed)

        sink.ack.on_next(continue_ack)
        self.scheduler.advance_by(1)
        self.assertEqual([1, 1], sink.received)
        self.assertFalse(sink.is_completed)

        sink.ack.on_next(continue_ack)
        self.scheduler.advance_by(1)
        self.assertEqual([1, 1, 2], sink.received)
        self.assertTrue(sink.is_completed)

    def test_emit_fill_and_empty_asynchronous_ack(self):
        sink = TestObserver(immediate_coninue=0)
        obs = MergeObservable(self.s1, self.s2)
        obs.observe(ObserverInfo(sink))

        left_ack = self.s1.on_next_single(1)
        self.assertEqual([1], sink.received)

        left_ack = self.s1.on_next_single(1)
        self.assertEqual([1], sink.received)

        sink.ack.on_next(continue_ack)
        self.scheduler.advance_by(1)
        self.assertEqual([1, 1], sink.received)

        sink.ack.on_next(continue_ack)
        self.scheduler.advance_by(1)
        self.assertEqual([1, 1], sink.received)

        self.s1.on_completed()
        self.assertFalse(sink.is_completed)

        self.s2.on_completed()
        self.assertTrue(sink.is_completed)