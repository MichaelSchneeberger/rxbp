import unittest

import rxbp
from rxbp.observerinfo import ObserverInfo
from rxbp.subscriber import Subscriber
from rxbp.testing.tobserver import TObserver
from rxbp.testing.tscheduler import TScheduler


class TestFromRange(unittest.TestCase):
    def setUp(self):
        self.scheduler = TScheduler()

    def test_happy_case(self):
        sink = TObserver(immediate_continue=0)
        subscription = rxbp.empty().unsafe_subscribe(Subscriber(
            scheduler=self.scheduler,
            subscribe_scheduler=self.scheduler,
        ))
        disposable = subscription.observable.observe(init_observer_info(
            observer=sink
        ))

        self.scheduler.advance_by(1)

        self.assertEqual([], sink.received)
        self.assertTrue(sink.is_completed)
