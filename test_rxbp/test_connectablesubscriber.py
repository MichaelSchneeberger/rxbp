import unittest

from rxbp.ack import Continue
from rxbp.observers.connectablesubscriber import ConnectableSubscriber
from rxbp.observer import Observer
from rxbp.schedulers.trampolinescheduler import TrampolineScheduler
from rxbp.testing.testobserver import TestObserver
from rxbp.testing.testscheduler import TestScheduler
from rxbp.testing.testcasebase import TestCaseBase


class TestConnectableSubscriber(TestCaseBase):

    def setUp(self):
        self.scheduler = TestScheduler()

    def test_should_block_onnext_until_connected(self):
        s: TestScheduler = self.scheduler

        o1 = TestObserver()
        down_stream = ConnectableSubscriber(o1, scheduler=s, subscribe_scheduler=TrampolineScheduler())

        gen_single = TestCaseBase.gen_single

        f = down_stream.on_next(gen_single(10))
        down_stream.on_completed()
        s.advance_by(1)

        self.assertFalse(o1.is_completed, 'f should not be completed')
        for i in range(9):
            down_stream.push_first(gen_single(i+1))

        s.advance_by(1)
        self.assertFalse(o1.is_completed, 'f should not be completed')
        self.assertEqual(len(o1.received), 0)

        o1.immediate_continue = 20
        down_stream.connect()
        s.advance_by(1)

        self.assertEqual(len(o1.received), 10)
        self.assertLessEqual(o1.received, [e+1 for e in range(10)])
        self.assertTrue(o1.is_completed, 'f should be completed')

    # def test_should_emit_pushed_items_immediately_after_connect(self):
    #     s: TestScheduler = self.scheduler
    #
    #     received = []
    #     was_completed = [False]
    #
    #     class TestObserver(Observer):
    #         def on_next(self, v):
    #             received.append(v)
    #             return Continue()
    #
    #         def on_error(self, err):
    #             pass
    #
    #         def on_completed(self):
    #             was_completed[0] = True
    #
    #     down_stream = ConnectableSubscriber(TestObserver(), scheduler=s)
    #
    #     down_stream.push_first(1)
    #     down_stream.push_first(2)
    #     down_stream.connect()
    #     s.advance_by(1)
    #
    #     self.assertEqual(len(received), 2)
    #     self.assertLessEqual(received, [e+1 for e in range(2)])
    #     self.assertFalse(was_completed[0])
    #
    # def test_should_not_allow_push_first_after_connect(self):
    #     s: TestScheduler = self.scheduler
    #
    #     class TestObserver(Observer):
    #         def on_next(self, v):
    #             return Continue()
    #
    #         def on_error(self, err):
    #             pass
    #
    #         def on_completed(self):
    #             pass
    #
    #     down_stream = ConnectableSubscriber(TestObserver(), scheduler=s)
    #
    #     down_stream.connect()
    #     s.advance_by(1)
    #
    #     with self.assertRaises(Exception):
    #         down_stream.push_first(1)
    #
    # def test_should_not_allow_push_first_all_after_connect(self):
    #     s: TestScheduler = self.scheduler
    #
    #     class TestObserver(Observer):
    #         def on_next(self, v):
    #             return Continue()
    #
    #         def on_error(self, err):
    #             pass
    #
    #         def on_completed(self):
    #             pass
    #
    #     down_stream = ConnectableSubscriber(TestObserver(), scheduler=s)
    #
    #     down_stream.connect()
    #     s.advance_by(1)
    #
    #     with self.assertRaises(Exception):
    #         down_stream.push_first_all(1)
    #
    # def test_should_schedule_push_complete(self):
    #     s: TestScheduler = self.scheduler
    #
    #     received = []
    #     was_completed = [False]
    #
    #     class TestObserver(Observer):
    #         def on_next(self, v):
    #             received.append(v)
    #             return Continue()
    #
    #         def on_error(self, err):
    #             pass
    #
    #         def on_completed(self):
    #             was_completed[0] = True
    #
    #     down_stream = ConnectableSubscriber(TestObserver(), scheduler=s)
    #
    #     down_stream.on_next(10)
    #     down_stream.push_first(1)
    #     down_stream.push_first(2)
    #     down_stream.push_completed()
    #     down_stream.connect()
    #
    #     s.advance_by(1)
    #
    #     self.assertEqual(len(received), 2)
    #     self.assertLessEqual(received, [e+1 for e in range(2)])
    #     self.assertTrue(was_completed[0])
    #
    # def test_should_not_allow_push_complete_after_connect(self):
    #     s: TestScheduler = self.scheduler
    #
    #     class TestObserver(Observer):
    #         def on_next(self, v):
    #             return Continue()
    #
    #         def on_error(self, err):
    #             pass
    #
    #         def on_completed(self):
    #             pass
    #
    #     down_stream = ConnectableSubscriber(TestObserver(), scheduler=s)
    #
    #     down_stream.connect()
    #     s.advance_by(1)
    #
    #     with self.assertRaises(Exception):
    #         down_stream._on_completed_or_error()

    # def test_should_schedule_push_error(self):
    #     s: TestScheduler = self.scheduler
    #
    #     received = []
    #     was_completed = [False]
    #
    #     class TestObserver(Observer):
    #         def on_next(self, v):
    #             received.append(v)
    #             return Continue()
    #
    #         def on_error(self, err):
    #             pass
    #
    #         def on_completed(self):
    #             was_completed[0] = True
    #
    #     down_stream = ConnectableSubscriber(TestObserver(), scheduler=s)
    #
    #     down_stream.on_next(10)
    #     down_stream.push_first(1)
    #     down_stream.push_first(2)
    #     down_stream.push_error(Exception('dummy exception'))
    #     down_stream.connect()
    #
    #     # s.advance_by(1)
    #     #
    #     # self.assertEqual(len(received), 2)
    #     # self.assertLessEqual(received, [e+1 for e in range(2)])
    #     # self.assertTrue(was_completed[0])