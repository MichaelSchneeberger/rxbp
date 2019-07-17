import unittest

from rxbp.ack import continue_ack
from rxbp.schedulers.trampolinescheduler import TrampolineScheduler
from rxbp.subjects.publishsubject import PublishSubject
from rxbp.testing.testobservable import TestObservable
from rxbp.testing.testobserver import TestObserver
from rxbp.testing.testobserversubscribeinner import TestObserverSubscribeInner
from rxbp.testing.testscheduler import TestScheduler
from backup.window import window


class TestWindowObservable(unittest.TestCase):

    scheduler: TestScheduler

    def setUp(self):
        scheduler = TestScheduler()

        s1 = TestObservable()
        s2 = TestObservable()

        o1 = TestObserverSubscribeInner(inner_selector=lambda v: v[1], scheduler=scheduler)
        o2 = TestObserver()

        left, right = window(s1, s2, lambda v1, v2: v2 <= v1[0], lambda v1, v2: v1[1] < v2)

        left.subscribe_observer(o1, scheduler, TrampolineScheduler())
        right.subscribe_observer(o2, scheduler, TrampolineScheduler())

        self.scheduler = scheduler
        self.left_in = s1
        self.right_in = s2
        self.left_out = o1
        self.right_out = o2

    def _get_gen(self, val):
        def gen():
            yield from val

        return gen

    def test_1(self):
        ack_left = self.left_in.on_next(self._get_gen([(10, 11), (11, 12), (12, 13)]))

        print(self.left_out.received)

        # left should emit immediately a PublishSubject
        self.assertEqual(len(self.left_out.received[0]), 3)
        self.assertEqual(self.left_out.received[0][2][0], (12, 13))
        self.assertIsInstance(self.left_out.received[0][0][1], PublishSubject)

        ack_right = self.right_in.on_next(self._get_gen([5.5, 6.5, 7.5]))

        # # right.is_lower should be selected;
        # # - (False, 4) should be send over right
        # # - ack is directly forwarded to right_in
        # self.assertListEqual(self.right_out.received, [(False, 4)])
        # self.assertEqual(self.right_out.ack, ack_right)

        # request next right
        continue_ack.subscribe(self.right_out.ack.on_next)
        ack_right = self.right_in.on_next(self._get_gen([8.5, 9.5, 10.5]))

        print(self.right_out.received)

        # # right.is_equal should be selected;
        # # - 5 should be send over inner left
        # # - (True, 5) should be send over right
        # self.assertEqual(self.left_out.inner_obs.received, [5])
        # self.assertListEqual(self.right_out.received, [(False, 4), (True, 5)])
        #
        # self.right_out.ack.on_next(Continue())
        # self.right_out.ack.on_completed()
        # self.assertFalse(ack_right.has_value)
        #
        # self.left_out.inner_obs.ack.on_next(Continue())
        # self.left_out.inner_obs.ack.on_completed()
        # self.assertIsInstance(ack_right.value, Continue)
        #
        # ack_right = self.right_in.on_next(6)
        #
        # # right.is_higher should be selected;
        # self.assertTrue(self.left_out.inner_obs.is_completed)
        #
        # self.left_out.ack.on_next(Continue())
        # self.left_out.ack.on_completed()
        #
        # self.assertIsInstance(ack_left.value, Continue)
        # self.assertEqual(self.left_out.inner_obs.received, [5])
        # self.assertListEqual(self.right_out.received, [(False, 4), (True, 5)])
        #
        # ack_left = self.left_in.on_next(6)
        #
        # # left.is_equal should be selected
        # self.assertEqual(self.left_out.received[1][0], 6)
        # self.assertListEqual(self.right_out.received, [(False, 4), (True, 5), (True, 6)])
        #
        # self.right_out.ack.on_next(Continue())
        # self.right_out.ack.on_completed()
        # self.left_out.inner_obs.ack.on_next(Continue())
        # self.left_out.inner_obs.ack.on_completed()
        #
        # self.assertIsInstance(ack_right.value, Continue)
        #
        # ack_right = self.right_in.on_next(7)
        #
        # # right.is_higher should be selected;
        # self.assertTrue(self.left_out.inner_obs.is_completed)
        #
        # self.left_out.ack.on_next(Continue())
        # self.left_out.ack.on_completed()
        #
        # self.assertIsInstance(ack_left.value, Continue)
        # self.assertEqual(self.left_out.inner_obs.received, [6])
        # self.assertListEqual(self.right_out.received, [(False, 4), (True, 5), (True, 6)])
        #
        # # ----------------------------------
        # #           left.is_higher
        #
        # ack_left = self.left_in.on_next(8)
        #
        # # left.is_higher should be selected
        # self.assertListEqual(self.right_out.received, [(False, 4), (True, 5), (True, 6), (False, 7)])
        # self.right_out.ack.on_next(Continue())
        # self.right_out.ack.on_completed()
        # self.assertIsInstance(ack_right.value, Continue)
        #
        # # ----------------------------------
        # #           left.is_lower
        #
        # ack_right = self.right_in.on_next(10)
        #
        # self.left_out.ack.on_next(Continue())
        # self.left_out.ack.on_completed()
        # ack_left = self.left_in.on_next(9)
        #
        # # left.is_lower should be selected
        # self.assertEqual(self.left_out.received[3][0], 9)
        # self.assertTrue(self.left_out.inner_obs.is_completed)
        # self.assertListEqual(self.right_out.received, [(False, 4), (True, 5), (True, 6), (False, 7)])
