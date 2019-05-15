import unittest

from rxbp.observables.zip2observable import Zip2Observable
from rxbp.testing.testcasebase import TestCaseBase
from rxbp.testing.testobservable import TestObservable
from rxbp.testing.testobserver import TestObserver
from rxbp.testing.testscheduler import TestScheduler


class TestConnectableSubscriber(TestCaseBase):
    """
    Zip2Observable is a stateful object, therefore we test methods of Zip2Observable as a function of its states
    called "zip_state" of type Zip2Observable.ZipState and "termination_state" of type Zip2Observable.TerminationState.
    The termination state has four data types (haskell terminology), which possibly have their own states.
    The zip state has five data types, which possibly have their own states as well.

    Restricted method calls according to rxbackpressure conventions:
    1. left.on_next and right.on_next in state=ZipElements
    2. left.on_next in state=WaitForRight
    3. right.on_next in state=WaitForLeft

    """

    def setUp(self):
        self.scheduler = TestScheduler()
        self.s1 = TestObservable()
        self.s2 = TestObservable()
        self.o = TestObserver()

    def test_basics(self):
        Zip2Observable(self.s1, self.s2) \
            .observe(self.o)

        gen_seq = self.gen_seq

        self.o.immediate_continue = 2

        self.s1.on_next(gen_seq([1, 2, 3]))
        self.assertListEqual(self.o.received, [])

        # state WaitLeft -> ZipElements -> WaitLeft
        self.s2.on_next(gen_seq([10, 11]))
        self.assertListEqual(self.o.received, [(1, 10), (2, 11)])

        # state InitState -> LeftCompletedState
        self.s1.on_completed()

        # state WaitLeft -> ZipElements -> WaitLeft
        self.s2.on_next(gen_seq([12, 13]))

        self.assertListEqual(self.o.received, [(1, 10), (2, 11), (3, 12)])
        self.assertTrue(self.o.is_completed)

    # def test_starts_before_other_and_finishes_before_other2(self):
    #     Zip2Observable(self.s1, self.s2) \
    #         .subscribe_observer(self.o, self.scheduler)
    #
    #     self.o.immediate_continue = 2
    #
    #     self.s1.on_next(1)
    #     self.scheduler.advance_by(1)
    #     self.assertListEqual(self.o.received, [])
    #     self.s2.on_next(2)
    #     self.scheduler.advance_by(1)
    #     self.assertListEqual(self.o.received, [(1, 2)])
    #
    #     self.s2.on_next(4)
    #     self.scheduler.advance_by(1)
    #     self.assertListEqual(self.o.received, [(1, 2)])
    #     self.s1.on_completed()
    #
    #     self.s1.on_next(3)
    #     self.scheduler.advance_by(1)
    #     self.assertListEqual(self.o.received, [(1, 2), (3, 4)])
    #
    #     self.scheduler.advance_by(1)
    #     self.assertTrue(self.o.is_completed)
    #
    # def test_signals_error_and_interrupts_the_stream_before_it_starts(self):
    #     Zip2Observable(self.s1, self.s2) \
    #         .subscribe_observer(self.o, self.scheduler)
    #
    #     self.o.immediate_continue = 2
    #
    #     dummy_exception = Exception('dummy')
    #     self.s1.on_error(dummy_exception)
    #     self.assertEqual(self.o.was_thrown, dummy_exception)
    #
    #     ack = self.s2.on_next(2)
    #     self.scheduler.advance_by(1)
    #     self.assertEqual(self.o.received, [])
    #     self.assertIsInstance(ack.value, Stop)
