import unittest

from rxbp.flowable import Flowable
from rxbp.multicast.multicastsubscriber import MultiCastSubscriber
from rxbp.multicast.multicasts.sharedmulticast import SharedMultiCast
from rxbp.multicast.testing.testmulticast import TestMultiCast
from rxbp.multicast.testing.testmulticastobserver import TestMultiCastObserver
from rxbp.testing.testflowable import TestFlowable
from rxbp.testing.testscheduler import TestScheduler


class TestShareMultiCast(unittest.TestCase):
    def setUp(self) -> None:
        self.multicast_scheduler = TestScheduler()
        self.source_scheduler = TestScheduler()
        self.info = MultiCastInfo(
            multicast_scheduler=self.multicast_scheduler,
            source_scheduler=self.source_scheduler,
        )
        self.source_multicast = TestMultiCast()
        self.rx_sink1 = TestMultiCastObserver()
        self.rx_sink2 = TestMultiCastObserver()
        self.source1 = TestFlowable()
        self.source2 = TestFlowable()

    def test_send_single_flowable(self):
        reduce_multicast = SharedMultiCast(source=self.source_multicast)
        observable = reduce_multicast.get_source(self.info)

        observable.subscribe(self.rx_sink1)
        observable.subscribe(self.rx_sink2)

        self.source_multicast.on_next(Flowable(self.source1))

        self.assertEqual(1, len(self.rx_sink1.received))
        self.assertEqual(1, len(self.rx_sink2.received))
