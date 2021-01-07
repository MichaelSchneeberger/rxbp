import unittest

import rxbp
from rxbp.acknowledgement.continueack import continue_ack
from rxbp.init.initobserverinfo import init_observer_info
from rxbp.init.initsubscriber import init_subscriber
from rxbp.testing.tobserver import TObserver
from rxbp.testing.tscheduler import TScheduler


class TestFromRange(unittest.TestCase):
    def setUp(self) -> None:
        self.scheduler = TScheduler()
        self.subscriber = init_subscriber(
            scheduler=self.scheduler,
            subscribe_scheduler=self.scheduler,
        )

    def test_use_case(self):
        sink = TObserver(immediate_continue=0)
        subscription = rxbp.from_range(1, 4).unsafe_subscribe(self.subscriber)
        subscription.observable.observe(init_observer_info(observer=sink))

        self.scheduler.advance_by(1)

        self.assertEqual([1, 2, 3], sink.received)
        self.assertTrue(sink.is_completed)

    def test_from_list_batch_size_of_one(self):
        sink = TObserver(immediate_continue=0)
        subscription = rxbp.from_range(1, 4, batch_size=1).unsafe_subscribe(self.subscriber)
        subscription.observable.observe(init_observer_info(observer=sink))

        self.scheduler.advance_by(1)

        self.assertEqual([1], sink.received)
        self.assertFalse(sink.is_completed)

        sink.ack.on_next(continue_ack)
        self.scheduler.advance_by(1)

        self.assertEqual([1, 2], sink.received)

    def test_from_list_batch_size_of_two(self):
        sink = TObserver(immediate_continue=0)
        subscription = rxbp.from_range(1, 4, batch_size=2).unsafe_subscribe(self.subscriber)
        subscription.observable.observe(init_observer_info(observer=sink))

        self.scheduler.advance_by(1)

        self.assertEqual([1, 2], sink.received)

        sink.ack.on_next(continue_ack)
        self.scheduler.advance_by(1)

        self.assertEqual([1, 2, 3], sink.received)
        self.assertTrue(sink.is_completed)