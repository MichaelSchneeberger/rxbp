import unittest

from rxbp.acknowledgement.continueack import ContinueAck
from rxbp.init.initobserverinfo import init_observer_info
from rxbp.observablesubjects.publishobservablesubject import PublishObservableSubject
from rxbp.observerinfo import ObserverInfo
from rxbp.testing.tobservable import TObservable
from rxbp.testing.tobserver import TObserver
from rxbp.testing.tscheduler import TScheduler


class TestPublishSubject(unittest.TestCase):

    def setUp(self):
        self.scheduler = TScheduler()
        self.exception = Exception()

    def test_should_emit_from_the_point_of_subscription_forward(self):
        subject = PublishObservableSubject(scheduler=self.scheduler)
        s1 = TObservable(observer=subject)

        self.assertIsInstance(s1.on_next_iter([1]), ContinueAck)
        self.assertIsInstance(s1.on_next_iter([2]), ContinueAck)
        self.assertIsInstance(s1.on_next_iter([3]), ContinueAck)

        o1 = TObserver()
        o1.immediate_continue = 5

        subject.observe(init_observer_info(o1))

        self.assertIsInstance(s1.on_next_iter([4]), ContinueAck)
        self.assertIsInstance(s1.on_next_iter([5]), ContinueAck)
        self.assertIsInstance(s1.on_next_iter([6]), ContinueAck)
        s1.on_completed()

        self.assertEqual(sum(o1.received), 15)
        self.assertTrue(o1.is_completed)

    def test_should_work_synchronously_for_synchronous_subscribers(self):
        subject = PublishObservableSubject(self.scheduler)
        s1 = TObservable(observer=subject)

        def gen_observers():
            for i in range(10):
                o1 = TObserver()
                o1.immediate_continue = 5
                subject.observe(init_observer_info(o1))
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
        subject = PublishObservableSubject(self.scheduler)
        subject.on_completed()

        o1 = TObserver()
        subject.observe(init_observer_info(o1))
        self.assertTrue(o1.is_completed)

    def test_on_error_should_terminate_current_and_future_subscribers(self):
        subject = PublishObservableSubject(self.scheduler)
        s1 = TObservable(observer=subject)

        def gen_observers():
            for _ in range(10):
                observer = TObserver()
                subject.observe(init_observer_info(
                    observer=observer,
                ))
                yield observer

        observer_list = list(gen_observers())

        s1.on_next_iter([1])
        s1.on_error(self.exception)

        o1 = TObserver()
        subject.observe(init_observer_info(o1))

        for observer in observer_list:
            self.assertListEqual(observer.received, [1])
            self.assertEqual(observer.exception, self.exception)

        self.assertEqual(o1.exception, self.exception)

    def test_unsubscribe_after_on_complete(self):
        subject = PublishObservableSubject(self.scheduler)
        s1 = TObservable(observer=subject)
        o1 = TObserver()
        d = subject.observe(init_observer_info(o1))

        s1.on_next_iter([1])
        s1.on_completed()

        self.scheduler.advance_by(1)
        d.dispose()
        self.assertListEqual(o1.received, [1])
