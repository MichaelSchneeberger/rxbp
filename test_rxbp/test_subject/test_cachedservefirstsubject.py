from rxbp.ack.ackimpl import Continue, continue_ack
from rxbp.observerinfo import ObserverInfo
from rxbp.observablesubjects.cacheservefirstosubject import CacheServeFirstOSubject
from rxbp.testing.testobservable import TestObservable
from rxbp.testing.testobserver import TestObserver
from rxbp.testing.testscheduler import TestScheduler
from rxbp.testing.testcasebase import TestCaseBase


class TestCachedServeFirstSubject(TestCaseBase):

    def setUp(self):
        self.scheduler = TestScheduler()
        self.source = TestObservable()
        self.subject = CacheServeFirstOSubject(scheduler=self.scheduler)
        self.source.observe(ObserverInfo(self.subject))

    def test_initialize(self):
        CacheServeFirstOSubject(scheduler=self.scheduler)

    def test_on_next_synchronously(self):
        """
                                     on_next
        inactive_subscriptions = [s1] -> inactive_subscriptions = [s1]
        """

        # preparation
        o1 = TestObserver()
        self.subject.observe(ObserverInfo(o1))

        # state change
        ack = self.source.on_next_single(1)

        # verification
        self.assertIsInstance(ack, Continue)
        self.assertEqual([1], o1.received)
        self.assertEqual(0, len(self.subject.shared_state.queue))

    def test_on_completed(self):
        """
               on_completed
        NormalState -> CompletedState
        """

        # preparation
        o1 = TestObserver()
        self.subject.observe(ObserverInfo(o1))

        # state change
        self.source.on_completed()

        # verification
        self.assertTrue(o1.is_completed)
        self.assertIsInstance(self.subject.state,
                              CacheServeFirstOSubject.CompletedState)

    def test_on_next_two_subscriber_synchronously(self):
        """
                                     on_next
        inactive_subscriptions = [s1, s2] -> inactive_subscriptions = [s1, s2]
        """

        # preparation
        o1 = TestObserver()
        o2 = TestObserver()
        self.subject.observe(ObserverInfo(o1))
        self.subject.observe(ObserverInfo(o2))

        # state change
        ack = self.source.on_next_single(1)

        # verification
        self.assertIsInstance(ack, Continue)
        self.assertEqual([1], o1.received)
        self.assertEqual([1], o2.received)
        self.assertEqual(0, len(self.subject.shared_state.queue))

    def test_on_next_assynchronously(self):
        """
                                     on_next
        inactive_subscriptions = [s1] -> inactive_subscriptions = []
        """

        # preparation
        o1 = TestObserver(immediate_coninue=0)
        self.subject.observe(ObserverInfo(o1))

        # state change
        ack = self.source.on_next_single(1)

        # verification
        self.assertNotEqual(continue_ack, ack)
        self.assertEqual([1], o1.received)

    def test_on_next_assynchronously2(self):
        """
                                ack.on_next
        inactive_subscriptions = [] -> inactive_subscriptions = [s1]
        """

        # preparation
        o1 = TestObserver(immediate_coninue=0)
        self.subject.observe(ObserverInfo(o1))
        ack = self.source.on_next_single(1)

        # state change
        o1.ack.on_next(continue_ack)
        self.scheduler.advance_by(1)

        # verification
        self.assertEqual(
            len(self.subject.shared_state.inactive_subscriptions),
            1
        )

    def test_on_next_multiple_elements_asynchronously(self):
        o1 = TestObserver(immediate_coninue=0)
        self.subject.observe(ObserverInfo(o1))

        self.source.on_next_single(1)
        self.source.on_next_single(2)
        self.assertEqual([1], o1.received)

    def test_on_next_assynchronously_enter_fast_loop(self):
        o1 = TestObserver(immediate_coninue=0)
        self.subject.observe(ObserverInfo(o1))

        self.source.on_next_single(1)
        self.source.on_next_single(2)

        # enter fast loop
        o1.ack.on_next(continue_ack)
        self.scheduler.advance_by(1)

        self.assertEqual([1, 2], o1.received)

    def test_on_next_assynchronously_enter_fast_loop2(self):
        o1 = TestObserver(immediate_coninue=0)
        self.subject.observe(ObserverInfo(o1))

        self.source.on_next_single(1)
        self.source.on_next_single(2)
        self.source.on_next_single(3)

        o1.immediate_continue = 1
        o1.ack.on_next(continue_ack)
        self.scheduler.advance_by(1)

        self.assertEqual([1, 2, 3], o1.received)

    def test_on_next_assynchronously_enter_fast_loop3(self):
        o1 = TestObserver(immediate_coninue=0)
        self.subject.observe(ObserverInfo(o1))

        self.source.on_next_single(1)
        self.source.on_next_single(2)
        self.source.on_next_single(3)

        o1.ack.on_next(continue_ack)
        self.scheduler.advance_by(1)

        o1.ack.on_next(continue_ack)
        self.scheduler.advance_by(1)

        self.assertEqual([1, 2, 3], o1.received)

    def test_on_next_multiple_elements_two_subscribers_asynchronously(self):
        o1 = TestObserver(immediate_coninue=0)
        o2 = TestObserver(immediate_coninue=0)
        self.subject.observe(ObserverInfo(o1))
        self.subject.observe(ObserverInfo(o2))

        self.source.on_next_single(1)
        self.source.on_next_single(2)

        self.assertEqual([1], o1.received)
        self.assertEqual([1], o2.received)

    def test_on_next_enter_fast_loop_two_subscribers_asynchronously(self):
        o1 = TestObserver(immediate_coninue=0)
        o2 = TestObserver(immediate_coninue=0)
        self.subject.observe(ObserverInfo(o1))
        self.subject.observe(ObserverInfo(o2))

        self.source.on_next_single(1)
        self.source.on_next_single(2)

        o1.ack.on_next(continue_ack)
        self.scheduler.advance_by(1)

        self.assertEqual([1, 2], o1.received)
        self.assertEqual([1], o2.received)

    def test_on_next_enter_fast_loop_two_subscribers_asynchronously2(self):
        o1 = TestObserver()
        o2 = TestObserver(immediate_coninue=0)
        self.subject.observe(ObserverInfo(o1))
        self.subject.observe(ObserverInfo(o2))

        self.source.on_next_single(1)
        self.source.on_next_single(2)

        o2.ack.on_next(continue_ack)
        self.scheduler.advance_by(1)

        o2.ack.on_next(continue_ack)
        self.scheduler.advance_by(1)

        self.source.on_next_single(3)

        self.assertEqual([1, 2, 3], o1.received)
        self.assertEqual([1, 2, 3], o2.received)

    # def test_should_block_onnext_until_connected2(self):
    #     s: TestScheduler = self.scheduler
    #
    #     o1 = TestObserver(immediate_coninue=0)
    #     o2 = TestObserver(immediate_coninue=0)
    #
    #     subject = CacheServeFirstOSubject(scheduler=s)
    #     subject.observe(ObserverInfo(o1))
    #     subject.observe(ObserverInfo(o2))
    #
    #     def gen_value(v):
    #         def gen():
    #             yield v
    #         return gen()
    #
    #     # -----------------
    #     # 2 inactive, one returns continue => subject returns continue
    #
    #     o1.immediate_continue = 1
    #     ack = subject.on_next(gen_value(10))
    #
    #     self.assertListEqual(o1.received, [10])
    #     self.assertListEqual(o2.received, [10])
    #     self.assertIsInstance(ack, Continue)
    #     self.assertEqual(len(subject.shared_state.inactive_subscriptions), 1)
    #
    #     # -----------------
    #     # 1 inactive, asynchroneous ackowledgement
    #
    #     o1.immediate_continue = 0
    #     ack = subject.on_next(gen_value(20))
    #
    #     self.assertListEqual(o1.received, [10, 20])
    #     self.assertListEqual(o2.received, [10])
    #     self.assertFalse(ack.has_value)
    #
    #     o1.ack.on_next(Continue())
    #     self.scheduler.advance_by(1)
    #
    #     self.assertTrue(ack.has_value)
    #     self.assertIsInstance(ack.value, Continue)
    #
    #     # -----------------
    #     # 1 inactive, revive other observer
    #
    #     o1.immediate_continue = 0
    #     ack = subject.on_next(gen_value(30))
    #
    #     self.assertListEqual(o1.received, [10, 20, 30])
    #     self.assertListEqual(o2.received, [10])
    #
    #     ack = subject.on_next(gen_value(40))
    #     ack = subject.on_next(gen_value(50))
    #
    #     o2.immediate_continue = 1
    #     o2.ack.on_next(Continue())
    #     s.advance_by(1)
    #
    #     self.assertListEqual(o1.received, [10, 20, 30])
    #     self.assertListEqual(o2.received, [10, 20, 30])
    #
    #     o2.immediate_continue = 1
    #     o2.ack.on_next(Continue())
    #     s.advance_by(1)
    #
    #     self.assertListEqual(o1.received, [10, 20, 30])
    #     self.assertListEqual(o2.received, [10, 20, 30, 40, 50])
    #
    #     self.assertFalse(ack.has_value)
    #
    #     o1.immediate_continue = 2
    #     o1.ack.on_next(Continue())
    #     s.advance_by(1)
    #
    #     self.assertTrue(ack.has_value)
    #     self.assertListEqual(o1.received, [10, 20, 30, 40, 50])
    #     self.assertListEqual(o2.received, [10, 20, 30, 40, 50])
    #
    #     ack = subject.on_next(gen_value(60))
    #
    #     o2.ack.on_next(Continue())
    #     s.advance_by(1)
    #
    #     self.assertListEqual(o1.received, [10, 20, 30, 40, 50, 60])
    #     self.assertListEqual(o2.received, [10, 20, 30, 40, 50, 60])
