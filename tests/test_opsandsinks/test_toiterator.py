import unittest

from rxbp.ack.continueack import ContinueAck
from rxbp.testing.testflowable import TestFlowable
from rxbp.testing.testscheduler import TestScheduler
from rxbp.toiterator import to_iterator


class TestToIterator(unittest.TestCase):
    def setUp(self) -> None:
        self.scheduler = TestScheduler()
        self.source = TestFlowable()

    def test_immediate_yield(self):
        iterator = to_iterator(
            source=self.source,
            scheduler=self.scheduler,
        )

        ack = self.source.on_next_single(1)

        val = next(iterator)

        self.assertIsInstance(ack, ContinueAck)
        self.assertEqual(1, val)

    def test_scheduled_yield(self):
        iterator = to_iterator(
            source=self.source,
            scheduler=self.scheduler,
        )

        def action(_, __):
            self.source.on_next_single(1)

        self.scheduler.schedule(action)

        self.scheduler.sleep(1)

        val = next(iterator)

        self.assertEqual(1, val)
