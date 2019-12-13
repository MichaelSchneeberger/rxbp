import unittest

import rxbp
from rx.testing import ReactiveTest
from rx.testing.mockobserver import MockObserver
from rxbp.multicast.multicastInfo import MultiCastInfo
from rxbp.testing.testscheduler import TestScheduler

on_next = ReactiveTest.on_next
on_completed = ReactiveTest.on_completed
on_error = ReactiveTest.on_error
subscribe = ReactiveTest.subscribe
subscribed = ReactiveTest.subscribed
disposed = ReactiveTest.disposed
created = ReactiveTest.created


class TestEmptySource(unittest.TestCase):
    def setUp(self) -> None:
        self.multicast_scheduler = TestScheduler()
        self.source_scheduler = TestScheduler()
        self.info = MultiCastInfo(
            multicast_scheduler=self.multicast_scheduler,
            source_scheduler=self.source_scheduler,
        )
        self.o = MockObserver(self.source_scheduler)

    def test_initialize(self):
        rxbp.multicast.empty()

    def test_send_item_on_subscribe_scheduler(self):
        mc = rxbp.multicast.empty()

        source = mc.get_source(self.info)
        source.subscribe(self.o)

        self.assertEqual(0, len(self.o.messages))

        self.multicast_scheduler.advance_by(1)

        assert self.o.messages == [
            on_completed(0),
        ]
