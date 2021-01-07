import unittest

from rxbp.multicast.multicastsubscriber import MultiCastSubscriber
from rxbp.multicast.multicasts.maptoiteratormulticast import MapToIteratorMultiCast
from rxbp.multicast.testing.testmulticast import TestMultiCast
from rxbp.multicast.testing.testmulticastobserver import TestMultiCastObserver
from rxbp.testing.testflowable import TestFlowable
from rxbp.testing.tscheduler import TScheduler


class TestDeferMultiCast(unittest.TestCase):
    def setUp(self) -> None:
        self.multicast_scheduler = TScheduler()
        self.source_scheduler = TScheduler()
        self.info = MultiCastInfo(
            multicast_scheduler=self.multicast_scheduler,
            source_scheduler=self.source_scheduler,
        )
        self.source_multicast = TestMultiCast()
        self.rx_sink = TestMultiCastObserver()
        self.source1 = TestFlowable()
        self.source2 = TestFlowable()

    def test_send_single_flowable(self):
        map_to_iterator_multicast = MapToIteratorMultiCast(
            source=self.source_multicast,
            func=lambda v: range(v),
        )
        map_to_iterator_multicast.get_source(self.info).subscribe(self.rx_sink)
        self.source_multicast.on_next(3)

        self.assertEqual([0, 1, 2], self.rx_sink.received)
