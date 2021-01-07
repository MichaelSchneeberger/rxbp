import unittest

import rxbp
from rxbp.flowable import Flowable
from rxbp.observerinfo import ObserverInfo
from rxbp.subscriber import Subscriber
from rxbp.testing.testflowable import TestFlowable
from rxbp.testing.tobserver import TObserver
from rxbp.testing.tscheduler import TScheduler


class TestZip(unittest.TestCase):
    def setUp(self) -> None:
        self.scheduler = TScheduler()
        self.sources = [TestFlowable(), TestFlowable(), TestFlowable()]

    def test_use_case(self):
        sink = TObserver()
        subscription = rxbp.merge(*[Flowable(e) for e in self.sources]).unsafe_subscribe(Subscriber(
            scheduler=self.scheduler, subscribe_scheduler=self.scheduler))
        subscription.observable.observe(init_observer_info(observer=sink))

        self.sources[0].observable.on_next_single(1)
        self.sources[1].observable.on_next_single(2)
        self.sources[2].observable.on_next_single(3)

        self.scheduler.advance_by(1)

        self.assertEqual([1, 2, 3], sink.received)
