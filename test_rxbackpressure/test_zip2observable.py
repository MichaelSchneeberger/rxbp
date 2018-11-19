import unittest

from rxbackpressure.ack import Continue, Stop
from rxbackpressure.observables.zip2observable import Zip2Observable
from rxbackpressure.testing.testobservable import TestObservable
from rxbackpressure.testing.testobserver import TestObserver
from rxbackpressure.testing.testscheduler import TestScheduler


class TestConnectableSubscriber(unittest.TestCase):

    def setUp(self):
        self.scheduler = TestScheduler()
        self.s1 = TestObservable()
        self.s2 = TestObservable()
        self.o = TestObserver()

    def test_starts_before_other_and_finishes_before_other(self):
        Zip2Observable(self.s1, self.s2) \
            .subscribe(self.o, self.scheduler)

        self.o.immediate_continue = 2

        self.s1.on_next(1)
        self.scheduler.advance_by(1)
        self.assertListEqual(self.o.received, [])
        self.s2.on_next(2)
        self.scheduler.advance_by(1)
        self.assertListEqual(self.o.received, [(1, 2)])

        self.s2.on_next(4)
        self.scheduler.advance_by(1)
        self.assertListEqual(self.o.received, [(1, 2)])
        self.s1.on_next(3)
        self.scheduler.advance_by(1)
        self.assertListEqual(self.o.received, [(1, 2), (3, 4)])

        self.s1.on_completed()
        self.scheduler.advance_by(1)
        self.assertTrue(self.o.is_completed)

    def test_starts_before_other_and_finishes_before_other2(self):
        Zip2Observable(self.s1, self.s2) \
            .subscribe(self.o, self.scheduler)

        self.o.immediate_continue = 2

        self.s1.on_next(1)
        self.scheduler.advance_by(1)
        self.assertListEqual(self.o.received, [])
        self.s2.on_next(2)
        self.scheduler.advance_by(1)
        self.assertListEqual(self.o.received, [(1, 2)])

        self.s2.on_next(4)
        self.scheduler.advance_by(1)
        self.assertListEqual(self.o.received, [(1, 2)])
        self.s1.on_completed()

        self.s1.on_next(3)
        self.scheduler.advance_by(1)
        self.assertListEqual(self.o.received, [(1, 2), (3, 4)])

        self.scheduler.advance_by(1)
        self.assertTrue(self.o.is_completed)

    def test_signals_error_and_interrupts_the_stream_before_it_starts(self):
        Zip2Observable(self.s1, self.s2) \
            .subscribe(self.o, self.scheduler)

        self.o.immediate_continue = 2

        dummy_exception = Exception('dummy')
        self.s1.on_error(dummy_exception)
        self.assertEqual(self.o.was_thrown, dummy_exception)

        ack = self.s2.on_next(2)
        self.scheduler.advance_by(1)
        self.assertEqual(self.o.received, [])
        self.assertIsInstance(ack.value, Stop)
