import unittest

from rxbp.ack.continueack import ContinueAck
from rxbp.observablesubjects.publishosubject import PublishOSubject
from rxbp.observerinfo import ObserverInfo
from rxbp.testing.testobservable import TestObservable
from rxbp.testing.testobserver import TestObserver
from rxbp.testing.testscheduler import TestScheduler


class TestPublishSubject(unittest.TestCase):

    def setUp(self):
        self.scheduler = TestScheduler()

    def test_should_emit_from_the_point_of_subscription_forward(self):
        subject = PublishOSubject(scheduler=self.scheduler)
        s1 = TestObservable(observer=subject)

        self.assertIsInstance(s1.on_next_iter([1]), ContinueAck)
        self.assertIsInstance(s1.on_next_iter([2]), ContinueAck)
        self.assertIsInstance(s1.on_next_iter([3]), ContinueAck)

        o1 = TestObserver()
        o1.immediate_continue = 5

        subject.observe(ObserverInfo(o1))

        self.assertIsInstance(s1.on_next_iter([4]), ContinueAck)
        self.assertIsInstance(s1.on_next_iter([5]), ContinueAck)
        self.assertIsInstance(s1.on_next_iter([6]), ContinueAck)
        s1.on_completed()

        self.assertEqual(sum(o1.received), 15)
        self.assertTrue(o1.is_completed)

    def test_should_work_synchronously_for_synchronous_subscribers(self):
        subject = PublishOSubject(self.scheduler)
        s1 = TestObservable(observer=subject)

        def gen_observers():
            for i in range(10):
                o1 = TestObserver()
                o1.immediate_continue = 5
                subject.observe(ObserverInfo(o1))
                yield o1

        obs_list = list(gen_observers())

        self.assertIsInstance(s1.on_next_iter([1]), ContinueAck)
        self.assertIsInstance(s1.on_next_iter([2]), ContinueAck)
        self.assertIsInstance(s1.on_next_iter([3]), ContinueAck)
        s1.on_completed()

        self.assertEqual(sum(sum(o.received) for o in obs_list), 60)
        self.assertTrue(all(o.is_completed for o in obs_list))

    # def test_should_work_with_asynchronous_subscribers(self):
    #     subject = PublishOSubject(self.scheduler)
    #     s1 = TestObservable(observer=subject)
    #
    #     def gen_observers():
    #         for i in range(10):
    #             o1 = TestObserver()
    #             subject.observe(ObserverInfo(o1))
    #             yield o1
    #
    #     obs_list = list(gen_observers())
    #
    #     for i in range(10):
    #         ack = s1.on_next_iter([i])
    #         self.assertFalse(ack.has_value)
    #
    #         for o in obs_list:
    #             o.ack.on_next(continue_ack)
    #
    #         self.assertTrue(ack.has_value)
    #         # todo: e+1??
    #         self.assertEqual(sum(sum(o.received) for o in obs_list), sum(e+1 for e in range(i)) * 10)
    #
    #     subject.on_completed()
    #     self.assertTrue(all(o.is_completed for o in obs_list))

    def test_subscribe_after_complete_should_complete_immediately(self):
        subject = PublishOSubject(self.scheduler)
        subject.on_completed()

        o1 = TestObserver()
        subject.observe(ObserverInfo(o1))
        self.assertTrue(o1.is_completed)

    def test_on_error_should_terminate_current_and_future_subscribers(self):
        subject = PublishOSubject(self.scheduler)
        s1 = TestObservable(observer=subject)
        dummy = Exception('dummy')

        def gen_observers():
            for i in range(10):
                o1 = TestObserver()
                subject.observe(ObserverInfo(o1))
                yield o1

        obs_list = list(gen_observers())

        s1.on_next_iter([1])
        s1.on_error(dummy)

        o1 = TestObserver()
        subject.observe(ObserverInfo(o1))

        for obs in obs_list:
            self.assertListEqual(obs.received, [1])
            self.assertEqual(obs.exception, dummy)

        self.assertEqual(o1.exception, dummy)

    def test_unsubscribe_after_on_complete(self):
        subject = PublishOSubject(self.scheduler)
        s1 = TestObservable(observer=subject)
        o1 = TestObserver()
        d = subject.observe(ObserverInfo(o1))

        s1.on_next_iter([1])
        s1.on_completed()

        self.scheduler.advance_by(1)
        d.dispose()
        self.assertListEqual(o1.received, [1])
