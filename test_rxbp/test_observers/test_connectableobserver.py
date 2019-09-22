import math

from rxbp.observers.connectableobserver import ConnectableObserver
from rxbp.schedulers.trampolinescheduler import TrampolineScheduler
from rxbp.testing.testobservable import TestObservable
from rxbp.testing.testobserver import TestObserver
from rxbp.testing.testscheduler import TestScheduler
from rxbp.testing.testcasebase import TestCaseBase


class TestConnectableObserver(TestCaseBase):

    def setUp(self):
        self.scheduler = TestScheduler()

    def _gen_value(self, v):
        def gen():
            yield v
        return gen

    def test_should_block_onnext_until_connected(self):
        s: TestScheduler = self.scheduler

        o1 = TestObserver()
        conn_obs = ConnectableObserver(o1, scheduler=s, subscribe_scheduler=TrampolineScheduler())
        s1 = TestObservable(observer=conn_obs)

        f = s1.on_next_iter([10])
        s1.on_completed()
        s.advance_by(1)

        self.assertFalse(o1.is_completed, 'f should not be completed')
        for i in range(9):
            conn_obs.push_first(self._gen_value(i+1))

        s.advance_by(1)
        self.assertFalse(o1.is_completed, 'f should not be completed')
        self.assertEqual(len(o1.received), 0)

        o1.immediate_continue = math.inf
        conn_obs.connect()
        s.advance_by(1)

        self.assertEqual(len(o1.received), 10)
        self.assertLessEqual(o1.received, [e+1 for e in range(10)])
        self.assertTrue(o1.is_completed, 'f should be completed')

    def test_should_emit_pushed_items_immediately_after_connect(self):
        o1 = TestObserver()
        conn_obs = ConnectableObserver(o1, scheduler=self.scheduler,
                                          subscribe_scheduler=self.scheduler)

        conn_obs.push_first(self._gen_value(1))
        conn_obs.push_first(self._gen_value(2))

        o1.immediate_continue = math.inf
        conn_obs.connect()
        self.scheduler.advance_by(1)

        self.assertEqual(len(o1.received), 2)
        self.assertLessEqual(o1.received, [e+1 for e in range(2)])
        self.assertFalse(o1.is_completed)

    def test_should_not_allow_push_first_after_connect(self):
        o1 = TestObserver()
        conn_obs = ConnectableObserver(o1, scheduler=self.scheduler,
                                          subscribe_scheduler=self.scheduler)

        o1.immediate_continue = math.inf
        conn_obs.connect()
        self.scheduler.advance_by(1)

        with self.assertRaises(Exception):
            conn_obs.push_first(self._gen_value(1))

    def test_should_not_allow_push_first_all_after_connect(self):
        o1 = TestObserver()
        conn_obs = ConnectableObserver(o1, scheduler=self.scheduler, subscribe_scheduler=self.scheduler)

        o1.immediate_continue = math.inf
        conn_obs.connect()
        self.scheduler.advance_by(1)

        with self.assertRaises(Exception):
            conn_obs.push_first_all(self._gen_value(1))

    def test_should_schedule_push_complete(self):
        o1 = TestObserver()
        conn_obs = ConnectableObserver(o1, scheduler=self.scheduler,
                                          subscribe_scheduler=self.scheduler)
        s1 = TestObservable(observer=conn_obs)

        s1.on_next_iter([10])
        conn_obs.push_first(self._gen_value(1))
        conn_obs.push_first(self._gen_value(2))
        conn_obs.push_complete()
        conn_obs.connect()

        o1.immediate_continue = math.inf
        self.scheduler.advance_by(1)

        self.assertEqual(len(o1.received), 2)
        self.assertLessEqual(o1.received, [e+1 for e in range(2)])
        self.assertTrue(o1.is_completed)

    def test_should_not_allow_push_complete_after_connect(self):
        s: TestScheduler = self.scheduler

        o1 = TestObserver()
        down_stream = ConnectableObserver(o1, scheduler=s, subscribe_scheduler=self.scheduler)

        down_stream.connect()
        s.advance_by(1)

        with self.assertRaises(Exception):
            down_stream._on_completed_or_error()

    def test_should_schedule_push_error(self):
        s: TestScheduler = self.scheduler

        received = []
        was_completed = [False]

        o1 = TestObserver()
        conn_obs = ConnectableObserver(TestObserver(), scheduler=s, subscribe_scheduler=self.scheduler)
        s1 = TestObservable(observer=conn_obs)

        s1.on_next_iter([10])
        conn_obs.push_first(1)
        conn_obs.push_first(2)
        conn_obs.push_error(Exception('dummy exception'))
        conn_obs.connect()

        # s.advance_by(1)
        #
        # self.assertEqual(len(received), 2)
        # self.assertLessEqual(received, [e+1 for e in range(2)])
        # self.assertTrue(was_completed[0])