import unittest

import rxbp
from rxbp.ack.continueack import continue_ack
from rxbp.observerinfo import ObserverInfo
from rxbp.selectors.bases.objectrefbase import ObjectRefBase
from rxbp.subscriber import Subscriber
from rxbp.testing.testobserver import TestObserver
from rxbp.testing.testscheduler import TestScheduler


class TestFromIterable(unittest.TestCase):
    def setUp(self) -> None:
        self.scheduler = TestScheduler()

    def test_base(self):
        test_list = [1, 2, 3]
        base = ObjectRefBase("test")

        subscription = rxbp.from_iterable(test_list, base=base).unsafe_subscribe(Subscriber(
            scheduler=self.scheduler, subscribe_scheduler=self.scheduler))

        self.assertEqual(base, subscription.info.base)

    def test_from_list(self):
        test_list = [1, 2, 3]

        sink = TestObserver(immediate_continue=0)
        subscription = rxbp.from_iterable(test_list).unsafe_subscribe(Subscriber(
            scheduler=self.scheduler, subscribe_scheduler=self.scheduler))
        subscription.observable.observe(ObserverInfo(observer=sink))

        self.scheduler.advance_by(1)

        self.assertEqual(test_list, sink.received)