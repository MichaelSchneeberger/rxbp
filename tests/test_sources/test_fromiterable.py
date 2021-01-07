import unittest

import rxbp
from rxbp.init.initobserverinfo import init_observer_info
from rxbp.init.initsubscriber import init_subscriber
from rxbp.testing.tobserver import TObserver
from rxbp.testing.tscheduler import TScheduler


class TestFromIterable(unittest.TestCase):
    def setUp(self) -> None:
        self.scheduler = TScheduler()
        self.subscriber = init_subscriber(
            scheduler=self.scheduler,
            subscribe_scheduler=self.scheduler,
        )

    def test_from_list(self):
        test_list = [1, 2, 3]

        sink = TObserver(immediate_continue=0)
        subscription = rxbp.from_iterable(test_list).unsafe_subscribe(self.subscriber)
        subscription.observable.observe(init_observer_info(observer=sink))

        self.scheduler.advance_by(1)

        self.assertEqual(test_list, sink.received)