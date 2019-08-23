from rxbp.ack.ack import Ack
from rxbp.ack.ackimpl import continue_ack
from rxbp.observables.flatmapobservable import FlatMapObservable
from rxbp.observables.zip2observable import Zip2Observable
from rxbp.observesubscription import ObserveSubscription
from rxbp.testing.testcasebase import TestCaseBase
from rxbp.testing.testobservable import TestObservable
from rxbp.testing.testobserver import TestObserver
from rxbp.testing.testscheduler import TestScheduler


class TestZip2Observable(TestCaseBase):

    def setUp(self):
        self.scheduler = TestScheduler()
        self.s1 = TestObservable()
        self.s2 = TestObservable()
        self.s3 = TestObservable()
        self.sink = TestObserver()

    def test_use_case_1_sync_ack(self):
        def selector(v):
            return v

        obs = FlatMapObservable(
            source=self.s1,
            selector=selector,
            scheduler=self.scheduler,
            subscribe_scheduler=self.scheduler
        )
        obs.observe(ObserveSubscription(self.sink))

        self.sink.immediate_continue = 10

        ack1 = self.s1.on_next_single(self.s2)

        ack2 = self.s2.on_next_seq([1, 2])
        self.assertListEqual(self.sink.received, [1, 2])

        self.s2.on_completed()

        self.assertTrue(ack1.has_value)
        ack1 = self.s1.on_next_single(self.s3)

        ack2 = self.s3.on_next_seq([3, 4])
        self.assertListEqual(self.sink.received, [1, 2, 3, 4])
