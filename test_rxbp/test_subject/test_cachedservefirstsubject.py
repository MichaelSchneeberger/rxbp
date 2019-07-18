from rxbp.ack.ackimpl import Continue
from rxbp.observesubscription import ObserveSubscription
from rxbp.subjects.cacheservefirstsubject import CacheServeFirstSubject
from rxbp.testing.testobserver import TestObserver
from rxbp.testing.testscheduler import TestScheduler
from rxbp.testing.testcasebase import TestCaseBase


class TestCachedServeFirstSubject(TestCaseBase):

    def setUp(self):
        self.scheduler = TestScheduler()

    def test_should_block_onnext_until_connected2(self):
        s: TestScheduler = self.scheduler

        o1 = TestObserver()
        o2 = TestObserver()

        subject = CacheServeFirstSubject(scheduler=s)
        subject.observe(ObserveSubscription(o1))
        subject.observe(ObserveSubscription(o2))

        def gen_value(v):
            def gen():
                yield v
            return gen

        # -----------------
        # 2 inactive, one returns continue => subject returns continue

        o1.immediate_continue = 1
        ack = subject.on_next(gen_value(10))

        self.assertListEqual(o1.received, [10])
        self.assertListEqual(o2.received, [10])
        self.assertIsInstance(ack, Continue)
        self.assertEqual(len(subject.inactive_subsriptions), 1)

        # -----------------
        # 1 inactive, asynchroneous ackowledgement

        o1.immediate_continue = 0
        ack = subject.on_next(gen_value(20))

        self.assertListEqual(o1.received, [10, 20])
        self.assertListEqual(o2.received, [10])
        self.assertFalse(ack.has_value)

        o1.ack.on_next(Continue())
        self.scheduler.advance_by(1)

        self.assertTrue(ack.has_value)
        self.assertIsInstance(ack.value, Continue)

        # -----------------
        # 1 inactive, revive other observer

        o1.immediate_continue = 0
        ack = subject.on_next(gen_value(30))

        self.assertListEqual(o1.received, [10, 20, 30])
        self.assertListEqual(o2.received, [10])

        ack = subject.on_next(gen_value(40))
        ack = subject.on_next(gen_value(50))

        o2.immediate_continue = 1
        o2.ack.on_next(Continue())
        s.advance_by(1)

        self.assertListEqual(o1.received, [10, 20, 30])
        self.assertListEqual(o2.received, [10, 20, 30])

        o2.immediate_continue = 1
        o2.ack.on_next(Continue())
        s.advance_by(1)

        self.assertListEqual(o1.received, [10, 20, 30])
        self.assertListEqual(o2.received, [10, 20, 30, 40, 50])

        self.assertFalse(ack.has_value)

        o1.immediate_continue = 2
        o1.ack.on_next(Continue())
        s.advance_by(1)

        self.assertTrue(ack.has_value)
        self.assertListEqual(o1.received, [10, 20, 30, 40, 50])
        self.assertListEqual(o2.received, [10, 20, 30, 40, 50])

        ack = subject.on_next(gen_value(60))

        o2.ack.on_next(Continue())
        s.advance_by(1)

        self.assertListEqual(o1.received, [10, 20, 30, 40, 50, 60])
        self.assertListEqual(o2.received, [10, 20, 30, 40, 50, 60])
