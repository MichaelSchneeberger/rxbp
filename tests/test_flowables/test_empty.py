import unittest

import rxbp
from rxbp.ack.continueack import continue_ack
from rxbp.observerinfo import ObserverInfo
from rxbp.subscriber import Subscriber
from rxbp.testing.testobserver import TestObserver
from rxbp.testing.testscheduler import TestScheduler


class TestFromRange(unittest.TestCase):
    def setUp(self):
        self.scheduler = TestScheduler()

    def test_happy_case(self):
        sink = TestObserver(immediate_continue=0)
        subscription = rxbp.empty().unsafe_subscribe(Subscriber(
            scheduler=self.scheduler,
            subscribe_scheduler=self.scheduler,
        ))
        disposable = subscription.observable.observe(ObserverInfo(
            observer=sink
        ))

        self.scheduler.advance_by(1)

        self.assertEqual([], sink.received)
        self.assertTrue(sink.is_completed)
