import unittest

import rxbp
from rxbp.flowable import Flowable
from rxbp.multicast.multicast import MultiCast
from rxbp.multicast.multicastInfo import MultiCastInfo
from rxbp.multicast.multicasts.loopflowablemulticast import LoopFlowableMultiCast
from rxbp.multicast.multicasts.maptoiteratormulticast import MapToIteratorMultiCast
from rxbp.multicast.multicasts.reducemulticast import ReduceMultiCast
from rxbp.multicast.testing.testmulticast import TestMultiCast
from rxbp.multicast.testing.testrxobserver import TestRxObserver
from rxbp.observerinfo import ObserverInfo
from rxbp.subscriber import Subscriber
from rxbp.testing.testflowable import TestFlowable
from rxbp.testing.testobserver import TestObserver
from rxbp.testing.testscheduler import TestScheduler


class TestDeferMultiCast(unittest.TestCase):
    def setUp(self) -> None:
        self.multicast_scheduler = TestScheduler()
        self.source_scheduler = TestScheduler()
        self.info = MultiCastInfo(
            multicast_scheduler=self.multicast_scheduler,
            source_scheduler=self.source_scheduler,
        )
        self.source_multicast = TestMultiCast()
        self.rx_sink = TestRxObserver()
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
