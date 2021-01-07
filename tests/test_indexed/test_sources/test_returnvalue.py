import unittest

import rxbp
from rxbp.indexed.selectors.bases.numericalbase import NumericalBase
from rxbp.subscriber import Subscriber
from rxbp.testing.tobserver import TObserver
from rxbp.testing.tscheduler import TScheduler


class TestReturnValue(unittest.TestCase):
    def setUp(self) -> None:
        self.scheduler = TScheduler()

    def test_base(self):
        subscription = rxbp.return_value(1).unsafe_subscribe(Subscriber(
            scheduler=self.scheduler, subscribe_scheduler=self.scheduler))

        self.assertEqual(NumericalBase(1), subscription.info.base)

    def test_use_case(self):
        sink = TObserver(immediate_continue=0)
        subscription = rxbp.return_value(1).unsafe_subscribe(Subscriber(
            scheduler=self.scheduler, subscribe_scheduler=self.scheduler))
        subscription.observable.observe(init_observer_info(observer=sink))

        self.scheduler.advance_by(1)

        self.assertEqual([1], sink.received)
