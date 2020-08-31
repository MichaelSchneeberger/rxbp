import unittest

import rxbp
from rxbp.init.initobserverinfo import init_observer_info
from rxbp.init.initsubscriber import init_subscriber
from rxbp.testing.testobserver import TestObserver
from rxbp.testing.testscheduler import TestScheduler


class TestReturnValue(unittest.TestCase):
    def setUp(self) -> None:
        self.scheduler = TestScheduler()
        self.subscriber = init_subscriber(
            scheduler=self.scheduler,
            subscribe_scheduler=self.scheduler,
        )

    def test_use_case(self):
        sink = TestObserver(immediate_continue=0)
        subscription = rxbp.return_value(1).unsafe_subscribe(self.subscriber)
        subscription.observable.observe(init_observer_info(observer=sink))

        self.scheduler.advance_by(1)

        self.assertEqual([1], sink.received)
