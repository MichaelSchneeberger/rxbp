import unittest

import rxbp
from rxbp.ack.continueack import continue_ack
from rxbp.observerinfo import ObserverInfo
from rxbp.selectors.bases.numericalbase import NumericalBase
from rxbp.subscriber import Subscriber
from rxbp.testing.testobserver import TestObserver
from rxbp.testing.testscheduler import TestScheduler


class TestReturnValue(unittest.TestCase):
    def setUp(self) -> None:
        self.scheduler = TestScheduler()

    def test_base(self):
        subscription = rxbp.return_value(1).unsafe_subscribe(Subscriber(
            scheduler=self.scheduler, subscribe_scheduler=self.scheduler))

        self.assertEqual(NumericalBase(1), subscription.info.base)

    def test_use_case(self):
        sink = TestObserver(immediate_continue=0)
        subscription = rxbp.return_value(1).unsafe_subscribe(Subscriber(
            scheduler=self.scheduler, subscribe_scheduler=self.scheduler))
        subscription.observable.observe(ObserverInfo(observer=sink))

        self.scheduler.advance_by(1)

        self.assertEqual([1], sink.received)
