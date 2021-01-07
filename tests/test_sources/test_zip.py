import unittest

import rxbp
from rxbp.init.initflowable import init_flowable
from rxbp.init.initobserverinfo import init_observer_info
from rxbp.init.initsubscriber import init_subscriber
from rxbp.testing.testflowable import TestFlowable
from rxbp.testing.tobserver import TObserver
from rxbp.testing.tscheduler import TScheduler


class TestZip(unittest.TestCase):
    def setUp(self) -> None:
        self.scheduler = TScheduler()
        self.sources = [TestFlowable(), TestFlowable(), TestFlowable()]
        self.subscriber = init_subscriber(
            scheduler=self.scheduler,
            subscribe_scheduler=self.scheduler,
        )

    def test_use_case(self):
        sink = TObserver()
        subscription = rxbp.zip(*[init_flowable(e) for e in self.sources]).unsafe_subscribe(self.subscriber)
        subscription.observable.observe(init_observer_info(observer=sink))

        self.sources[0].observable.on_next_single(1)
        self.sources[1].observable.on_next_single(2)
        self.sources[2].observable.on_next_single(3)

        self.scheduler.advance_by(1)

        self.assertEqual([(1, 2, 3)], sink.received)
