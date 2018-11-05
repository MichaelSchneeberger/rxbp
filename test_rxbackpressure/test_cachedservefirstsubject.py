import unittest

from rxbackpressure.ack import Continue, Ack
from rxbackpressure.subjects.cachedservefirstsubject import CachedServeFirstSubject
from rxbackpressure.observer import Observer
from rxbackpressure.schedulers.currentthreadscheduler import CurrentThreadScheduler
from rxbackpressure.testing.testobserver import TestObserver
from rxbackpressure.testing.testscheduler import TestScheduler



class TestConnectableSubscriber(unittest.TestCase):

    def setUp(self):
        self.scheduler = TestScheduler()

    # def test_should_block_onnext_until_connected(self):
    #     s: TestScheduler = self.scheduler
    #
    #     o1 = TestObserver()
    #     o2 = TestObserver()
    #
    #     subject = CachedServeFirstSubject(scheduler=s)
    #     subject.subscribe(o1, s, CurrentThreadScheduler())
    #     subject.subscribe(o2, s, CurrentThreadScheduler())
    #
    #     ack = subject.on_next(10)
    #
    #     self.assertListEqual(o1.received, [10])
    #     self.assertListEqual(o2.received, [10])
    #     self.assertFalse(ack.has_value)
    #
    #     o2.ack.on_next(Continue())
    #     o2.ack.on_completed()
    #
    #     s.advance_by(1)
    #
    #     self.assertTrue(False)
    #
    #     # self.assertTrue(ack.has_value)
    #     # self.assertIsInstance(ack.value, Continue)
    #     #
    #     # ack = subject.on_next(20)
    #     #
    #     # self.assertListEqual(o1.received, [10])
    #     # self.assertListEqual(o2.received, [10, 20])
    #     # self.assertFalse(ack.has_value)
    #     #
    #     # o2.ack.on_next(Continue())
    #     # o2.ack.on_completed()
    #     #
    #     # s.advance_by(1)
    #     #
    #     # self.assertTrue(ack.has_value)
    #     # self.assertIsInstance(ack.value, Continue)
    #     #
    #     # ack = subject.on_next(30)
    #     #
    #     # self.assertListEqual(o1.received, [10])
    #     # self.assertListEqual(o2.received, [10, 20, 30])
    #     # self.assertFalse(ack.has_value)
    #     #
    #     # o1.ack.on_next(Continue())
    #     # o1.ack.on_completed()
    #     #
    #     # s.advance_by(1)
    #     #
    #     # self.assertListEqual(o1.received, [10, 20])
    #     # self.assertListEqual(o2.received, [10, 20, 30])
    #     # self.assertFalse(ack.has_value)

    def test_should_block_onnext_until_connected2(self):
        s: TestScheduler = self.scheduler

        o1 = TestObserver()
        o2 = TestObserver()

        subject = CachedServeFirstSubject(scheduler=s)
        subject.subscribe(o1, s, CurrentThreadScheduler())
        subject.subscribe(o2, s, CurrentThreadScheduler())

        o1.immediate_continue = 1
        ack = subject.on_next(10)

        self.assertListEqual(o1.received, [10])
        self.assertListEqual(o2.received, [10])
        self.assertTrue(ack.has_value)
        self.assertIsInstance(ack.value, Continue)

        o1.immediate_continue = 1
        ack = subject.on_next(20)

        self.assertListEqual(o1.received, [10, 20])
        self.assertListEqual(o2.received, [10])
        self.assertTrue(ack.has_value)
        self.assertIsInstance(ack.value, Continue)

        o1.immediate_continue = 0
        ack = subject.on_next(30)

        self.assertListEqual(o1.received, [10, 20, 30])
        self.assertListEqual(o2.received, [10])

        ack = subject.on_next(40)
        ack = subject.on_next(50)

        o2.immediate_continue = 4
        o2.ack.on_next(Continue())
        o2.ack.on_completed()

        s.advance_by(1)

        self.assertListEqual(o1.received, [10, 20, 30])
        self.assertListEqual(o2.received, [10, 20, 30, 40, 50])

    # def test_should_block_onnext_until_connected3(self):
    #     s: TestScheduler = self.scheduler
    #
    #     o1 = TestObserver()
    #     o2 = TestObserver()
    #
    #     subject = CachedServeFirstSubject(scheduler=s)
    #     subject.subscribe(o1, s, CurrentThreadScheduler())
    #     subject.subscribe(o2, s, CurrentThreadScheduler())
    #
    #     o1.immediate_continue = 1
    #     ack = subject.on_next(10)
    #
    #     self.assertListEqual(o1.received, [10])
    #     self.assertListEqual(o2.received, [10])
    #     self.assertTrue(ack.has_value)
    #     self.assertIsInstance(ack.value, Continue)
    #
    #     o1.immediate_continue = 1
    #     ack = subject.on_next(20)
    #
    #     self.assertListEqual(o1.received, [10, 20])
    #     self.assertListEqual(o2.received, [10])
    #     self.assertTrue(ack.has_value)
    #     self.assertIsInstance(ack.value, Continue)
    #
    #     o1.immediate_continue = 1
    #     ack = subject.on_next(30)
    #
    #     self.assertListEqual(o1.received, [10, 20, 30])
    #     self.assertListEqual(o2.received, [10])
    #
    #     o1.immediate_continue = 1
    #     ack = subject.on_next(40)
    #
    #     self.assertListEqual(o1.received, [10, 20, 30, 40])
    #     self.assertListEqual(o2.received, [10])
    #
    #     o2.immediate_continue = 1
    #     o2.ack.on_next(Continue())
    #     o2.ack.on_completed()
    #
    #     s.advance_by(1)
    #
    #     self.assertListEqual(o1.received, [10, 20, 30, 40])
    #     self.assertListEqual(o2.received, [10, 20, 30])
    #
    #     o2.ack.on_next(Continue())
    #     o2.ack.on_completed()
    #
    #     s.advance_by(1)
    #
    #     self.assertListEqual(o2.received, [10, 20, 30, 40])
