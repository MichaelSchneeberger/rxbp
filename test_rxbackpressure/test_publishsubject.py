import unittest

from rxbackpressure.ack import Continue, continue_ack
from rxbackpressure.schedulers.currentthreadscheduler import CurrentThreadScheduler
from rxbackpressure.subjects.publishsubject import PublishSubject
from rxbackpressure.testing.testobserver import TestObserver
from rxbackpressure.testing.testscheduler import TestScheduler



class TestPublishSubject(unittest.TestCase):

    def setUp(self):
        self.scheduler = TestScheduler()

    def test_should_emit_from_the_point_of_subscription_forward(self):
        subject = PublishSubject()

        self.assertIsInstance(subject.on_next(1), Continue)
        self.assertIsInstance(subject.on_next(2), Continue)
        self.assertIsInstance(subject.on_next(3), Continue)

        o1 = TestObserver()
        o1.immediate_continue = 5

        subject.unsafe_subscribe(o1, self.scheduler, CurrentThreadScheduler())

        self.assertIsInstance(subject.on_next(4), Continue)
        self.assertIsInstance(subject.on_next(5), Continue)
        self.assertIsInstance(subject.on_next(6), Continue)
        subject.on_completed()

        self.assertEqual(sum(o1.received), 15)
        self.assertTrue(o1.is_completed)

    def test_should_work_synchronously_for_synchronous_subscribers(self):
        subject = PublishSubject()

        def gen_observers():
            for i in range(10):
                o1 = TestObserver()
                o1.immediate_continue = 5
                subject.subscribe(o1, self.scheduler)
                yield o1

        obs_list = list(gen_observers())

        self.assertIsInstance(subject.on_next(1), Continue)
        self.assertIsInstance(subject.on_next(2), Continue)
        self.assertIsInstance(subject.on_next(3), Continue)
        subject.on_completed()

        self.assertEqual(sum(sum(o.received) for o in obs_list), 60)
        self.assertTrue(all(o.is_completed for o in obs_list))

    def test_should_work_with_asynchronous_subscribers(self):
        subject = PublishSubject()

        def gen_observers():
            for i in range(10):
                o1 = TestObserver()
                subject.subscribe(o1, self.scheduler)
                yield o1

        obs_list = list(gen_observers())

        for i in range(10):
            ack = subject.on_next(i)
            self.assertFalse(ack.has_value)

            for o in obs_list:
                o.ack.on_next(continue_ack)
                o.ack.on_completed()

            self.assertTrue(ack.has_value)
            # todo: e+1??
            self.assertEqual(sum(sum(o.received) for o in obs_list), sum(e+1 for e in range(i)) * 10)

        subject.on_completed()
        self.assertTrue(all(o.is_completed for o in obs_list))

    def test_subscribe_after_complete_should_complete_immediately(self):
        subject = PublishSubject()
        subject.on_completed()

        o1 = TestObserver()
        subject.subscribe(o1, self.scheduler)
        self.assertTrue(o1.is_completed)

    def test_on_error_should_terminate_current_and_future_subscribers(self):
        subject = PublishSubject()
        dummy = Exception('dummy')

        def gen_observers():
            for i in range(10):
                o1 = TestObserver()
                subject.subscribe(o1, self.scheduler)
                yield o1

        obs_list = list(gen_observers())

        subject.on_next(1)
        subject.on_error(dummy)

        o1 = TestObserver()
        subject.subscribe(o1, self.scheduler)

        for obs in obs_list:
            self.assertListEqual(obs.received, [1])
            self.assertEqual(obs.was_thrown, dummy)

        self.assertEqual(o1.was_thrown, dummy)

    def test_unsubscribe_after_on_complete(self):
        subject = PublishSubject()
        o1 = TestObserver()
        d = subject.subscribe(o1, self.scheduler)

        subject.on_next(1)
        subject.on_completed()

        self.scheduler.advance_by(1)
        d.dispose()
        self.assertListEqual(o1.received, [1])
