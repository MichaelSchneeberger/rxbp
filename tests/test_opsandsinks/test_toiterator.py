import unittest

from rxbp.acknowledgement.continueack import ContinueAck
from rxbp.init.initflowable import init_flowable
from rxbp.testing.testflowable import TestFlowable
from rxbp.testing.tscheduler import TScheduler
from rxbp.toiterator import to_iterator


class TestToIterator(unittest.TestCase):
    def setUp(self) -> None:
        self.scheduler = TScheduler()
        self.source = TestFlowable()

    def test_immediate_yield(self):
        iterator = to_iterator(
            source=init_flowable(self.source),
            scheduler=self.scheduler,
        )

        ack = self.source.on_next_single(1)

        val = next(iterator)

        self.assertIsInstance(ack, ContinueAck)
        self.assertEqual(1, val)

    def test_scheduled_yield(self):
        iterator = to_iterator(
            source=init_flowable(self.source),
            scheduler=self.scheduler,
        )

        def action(_, __):
            self.source.on_next_single(1)

        self.scheduler.schedule(action)

        self.scheduler.sleep(1)

        val = next(iterator)

        self.assertEqual(1, val)
