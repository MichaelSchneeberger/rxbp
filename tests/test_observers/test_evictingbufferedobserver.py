from rxbp.ack.continueack import ContinueAck, continue_ack
from rxbp.observers.evictingbufferedobserver import EvictingBufferedObserver
from rxbp.overflowstrategy import DropOld
from rxbp.testing.testcasebase import TestCaseBase
from rxbp.testing.testobservable import TestObservable
from rxbp.testing.testobserver import TestObserver
from rxbp.testing.testscheduler import TestScheduler


class TestEvictingBufferedObserver(TestCaseBase):

    def setUp(self):
        self.scheduler = TestScheduler()
        self.sink = TestObserver(immediate_continue=0)

    def test_should_block_onnext_until_connected(self):
        s: TestScheduler = self.scheduler

        strategy = DropOld(4)
        evicting_obs = EvictingBufferedObserver(self.sink, scheduler=s, strategy=strategy, subscribe_scheduler=s)
        s1 = TestObservable(observer=evicting_obs)

        s1.on_next_single(1)
        s1.on_next_single(2)
        ack = s1.on_next_single(3)
        self.assertIsInstance(ack, ContinueAck)

        ack = s1.on_next_single(4)
        self.assertIsInstance(ack, ContinueAck)

        ack = s1.on_next_single(5)
        self.assertIsInstance(ack, ContinueAck)

        self.assertEqual(len(self.sink.received), 0)

        self.scheduler.advance_by(1)

        self.assertEqual([2], self.sink.received)

        self.sink.ack.on_next(continue_ack)

        self.scheduler.advance_by(1)

        self.assertEqual(self.sink.received, [2, 3])
