import math

from rxbp.ack import Continue, continue_ack
from rxbp.observers.connectableobserver import ConnectableObserver
from rxbp.observers.evictingbufferedobserver import EvictingBufferedObserver
from rxbp.overflowstrategy import DropOld
from rxbp.schedulers.trampolinescheduler import TrampolineScheduler
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
        source = TestObservable()

        strategy = DropOld(4)
        down_stream = EvictingBufferedObserver(o1, scheduler=s, strategy=strategy)
        source.observe(down_stream)

        source.on_next_single(1)
        source.on_next_single(2)
        ack = source.on_next_single(3)
        self.assertIsInstance(ack, Continue)

        ack = source.on_next_single(4)
        self.assertIsInstance(ack, Continue)

        ack = source.on_next_single(5)
        self.assertIsInstance(ack, Continue)

        self.assertEqual(len(o1.received), 0)

        self.scheduler.advance_by(1)

        self.assertEqual(o1.received, [2])

        o1.ack.on_next(continue_ack)
        o1.ack.on_completed()

        self.scheduler.advance_by(1)

        self.assertEqual(o1.received, [2, 3])
