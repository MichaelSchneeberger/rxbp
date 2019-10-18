from rxbp.ack.ackimpl import Continue
from rxbp.multicast.observables.flatmapnobackpressureobservable import FlatMapNoBackpressureObservable
from rxbp.observerinfo import ObserverInfo
from rxbp.testing.testcasebase import TestCaseBase
from rxbp.testing.testobservable import TestObservable
from rxbp.testing.testobserver import TestObserver
from rxbp.testing.testscheduler import TestScheduler


class TestFlatConcatObservable(TestCaseBase):

    def setUp(self):
        self.scheduler = TestScheduler()
        self.s1 = TestObservable()
        self.s2 = TestObservable()
        self.s3 = TestObservable()

    def test_happy_path_sync_ack(self):
        sink = TestObserver()
        obs = FlatMapNoBackpressureObservable(
            source=self.s1,
            scheduler=self.scheduler,
            subscribe_scheduler=self.scheduler
        )
        obs.observe(ObserverInfo(sink))

        ack = self.s1.on_next_single(self.s2)
        self.assertIsInstance(ack, Continue)

        ack = self.s2.on_next_single(1)

        self.assertIsInstance(ack, Continue)
        self.assertListEqual(sink.received, [1])

        self.s1.on_next_single(self.s3)

        self.s3.on_next_single(2)
        self.assertListEqual(sink.received, [1])

        self.s2.on_completed()
        self.scheduler.advance_by(1)
        self.assertListEqual(sink.received, [1, 2])
