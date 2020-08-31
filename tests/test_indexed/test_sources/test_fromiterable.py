import unittest

import rxbp
from rxbp.init.initsubscriber import init_subscriber
from rxbp.indexed.selectors.bases.objectrefbase import ObjectRefBase
from rxbp.subscriber import Subscriber
from rxbp.testing.testobserver import TestObserver
from rxbp.testing.testscheduler import TestScheduler


class TestFromIterable(unittest.TestCase):
    def setUp(self) -> None:
        self.scheduler = TestScheduler()
        self.subscriber = init_subscriber(
            scheduler=self.scheduler,
            subscribe_scheduler=self.scheduler,
        )

    def test_base(self):
        test_list = [1, 2, 3]
        base = ObjectRefBase("test")

        subscription = rxbp.from_iterable(test_list).unsafe_subscribe(self.subscriber)

        self.assertEqual(base, subscription.info.base)

    def test_from_list(self):
        test_list = [1, 2, 3]

        sink = TestObserver(immediate_continue=0)
        subscription = rxbp.from_iterable(test_list).unsafe_subscribe(Subscriber(
            scheduler=self.scheduler, subscribe_scheduler=self.scheduler))
        subscription.observable.observe(init_observer_info(observer=sink))

        self.scheduler.advance_by(1)

        self.assertEqual(test_list, sink.received)