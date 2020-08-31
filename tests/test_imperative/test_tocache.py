import unittest

from rxbp.acknowledgement.continueack import ContinueAck
from rxbp.acknowledgement.stopack import StopAck
from rxbp.flowable import Flowable
from rxbp.imperative import to_cache
from rxbp.init.initflowable import init_flowable
from rxbp.testing.testflowable import TestFlowable
from rxbp.testing.testscheduler import TestScheduler


class TestOpsAndSinks(unittest.TestCase):
    def setUp(self) -> None:
        self.scheduler = TestScheduler()
        self.source = TestFlowable()

    def test_common_case(self):
        cache = to_cache(source=init_flowable(self.source), scheduler=self.scheduler)

        ack1 = self.source.on_next_single(1)
        ack2 = self.source.on_next_single(2)

        values = cache.to_list()

        ack3 = self.source.on_next_single(3)

        self.assertIsInstance(ack1, ContinueAck)
        self.assertIsInstance(ack2, ContinueAck)

        self.assertEqual([1, 2], values)

        self.assertIsInstance(ack3, StopAck)
