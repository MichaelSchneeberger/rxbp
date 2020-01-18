from rxbp.ack.continueack import ContinueAck
from rxbp.observables.flatmapobservable import FlatMapObservable
from rxbp.observerinfo import ObserverInfo
from rxbp.states.measuredstates.flatmapstates import FlatMapStates
from rxbp.testing.testcasebase import TestCaseBase
from rxbp.testing.testobservable import TestObservable
from rxbp.testing.testobserver import TestObserver
from rxbp.testing.testscheduler import TestScheduler


class TestFlatMapObservable(TestCaseBase):

    def setUp(self):
        self.scheduler = TestScheduler()
        self.s1 = TestObservable()
        self.s2 = TestObservable()
        self.s3 = TestObservable()
        self.exception = Exception('dummy exception')

    def test_happy_path_sync_ack(self):
        def selector(v):
            return v

        sink = TestObserver()
        obs = FlatMapObservable(
            source=self.s1,
            func=selector,
            scheduler=self.scheduler,
            subscribe_scheduler=self.scheduler
        )
        obs.observe(ObserverInfo(sink))

        ack1 = self.s1.on_next_single(self.s2)
        self.assertFalse(ack1.has_value)

        ack2 = self.s2.on_next_iter([1, 2])
        self.assertIsInstance(ack2, ContinueAck)

        self.assertListEqual(sink.received, [1, 2])

        self.s2.on_completed()
        self.assertIsInstance(ack1.value, ContinueAck)
        self.assertIsInstance(obs.state.get_measured_state(), FlatMapStates.WaitOnOuter)

        ack1 = self.s1.on_next_single(self.s3)
        ack2 = self.s3.on_next_iter([3, 4])
        self.assertIsInstance(ack2, ContinueAck)
        self.assertListEqual(sink.received, [1, 2, 3, 4])

        self.s3.on_completed()
        self.assertIsInstance(ack1.value, ContinueAck)
        self.assertIsInstance(obs.state.get_measured_state(), FlatMapStates.WaitOnOuter)

        self.s1.on_completed()
        self.assertTrue(sink.is_completed)

    def test_complete_outer_before_inner_sync_ack(self):
        def selector(v):
            return v

        sink = TestObserver()
        obs = FlatMapObservable(
            source=self.s1,
            func=selector,
            scheduler=self.scheduler,
            subscribe_scheduler=self.scheduler
        )
        obs.observe(ObserverInfo(sink))

        self.s1.on_next_single(self.s2)
        self.s1.on_completed()

        self.s2.on_completed()
        self.assertTrue(sink.is_completed)

    def test_exception_case(self):
        def selector(v):
            raise self.exception

        sink = TestObserver()
        obs = FlatMapObservable(
            source=self.s1,
            func=selector,
            scheduler=self.scheduler,
            subscribe_scheduler=self.scheduler
        )
        obs.observe(ObserverInfo(sink))

        self.s1.on_next_single(self.s2)

        self.assertEqual(self.exception, sink.exception)

    def test_multible_inner_sync_ack(self):
        def selector(v):
            return v

        sink = TestObserver()
        obs = FlatMapObservable(
            source=self.s1,
            func=selector,
            scheduler=self.scheduler,
            subscribe_scheduler=self.scheduler
        )
        obs.observe(ObserverInfo(sink))

        ack1 = self.s1.on_next_list([self.s2, self.s3])
        self.assertFalse(ack1.has_value)

        self.s1.on_completed()

        ack2 = self.s2.on_next_iter([1, 2])
        self.assertIsInstance(ack2, ContinueAck)

        ack3 = self.s3.on_next_iter([3, 4])
        self.assertFalse(ack3.has_value)
        self.assertListEqual(sink.received, [1, 2])

        self.s2.on_completed()

        self.scheduler.advance_by(1)
        self.assertListEqual(sink.received, [1, 2, 3, 4])

        self.s3.on_completed()
        self.assertTrue(sink.is_completed)
