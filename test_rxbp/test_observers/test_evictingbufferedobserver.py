from rxbp.ack.ackimpl import Continue, continue_ack
from rxbp.observers.evictingbufferedobserver import EvictingBufferedObserver
from rxbp.overflowstrategy import DropOld
from rxbp.testing.testobservable import TestObservable
from rxbp.testing.testobserver import TestObserver
from rxbp.testing.testscheduler import TestScheduler
from rxbp.testing.testcasebase import TestCaseBase


class TestEvictingBufferedObserver(TestCaseBase):

    def setUp(self):
        self.scheduler = TestScheduler()

    def test_should_block_onnext_until_connected(self):
        s: TestScheduler = self.scheduler

        o1 = TestObserver()

        strategy = DropOld(4)
        evicting_obs = EvictingBufferedObserver(o1, scheduler=s, strategy=strategy, subscribe_scheduler=s)
        s1 = TestObservable(observer=evicting_obs)

        s1.on_next_single(1)
        s1.on_next_single(2)
        ack = s1.on_next_single(3)
        self.assertIsInstance(ack, Continue)

        ack = s1.on_next_single(4)
        self.assertIsInstance(ack, Continue)

        ack = s1.on_next_single(5)
        self.assertIsInstance(ack, Continue)

        self.assertEqual(len(o1.received), 0)

        self.scheduler.advance_by(1)

        self.assertEqual(o1.received, [2])

        o1.ack.on_next(continue_ack)

        self.scheduler.advance_by(1)

        self.assertEqual(o1.received, [2, 3])
