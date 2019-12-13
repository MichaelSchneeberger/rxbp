import unittest

from rx.testing import ReactiveTest
from rxbp.flowable import Flowable
from rxbp.multicast.multicastInfo import MultiCastInfo
from rxbp.multicast.multicasts.reducemulticast import ReduceMultiCast
from rxbp.multicast.testing.testmulticast import TestMultiCast
from rxbp.multicast.testing.testrxobserver import TestRxObserver
from rxbp.observerinfo import ObserverInfo
from rxbp.subscriber import Subscriber
from rxbp.testing.testflowable import TestFlowable
from rxbp.testing.testobserver import TestObserver
from rxbp.testing.testscheduler import TestScheduler


class TestReduceMultiCast(unittest.TestCase):
    def setUp(self) -> None:
        self.multicast_scheduler = TestScheduler()
        self.source_scheduler = TestScheduler()
        self.info = MultiCastInfo(
            multicast_scheduler=self.multicast_scheduler,
            source_scheduler=self.source_scheduler,
        )
        # self.rx_sink = TestRxObserver()
        # self.sink = TestObserver()

    def test_1(self):
        rx_sink = TestRxObserver()
        source_multicast = TestMultiCast()
        reduce_multicast = ReduceMultiCast(ReduceMultiCast(source=source_multicast))
        reduce_multicast.get_source(self.info).subscribe(rx_sink)
        source_flowable = TestFlowable()

        source_multicast.on_next(Flowable(source_flowable))

        self.assertEqual(1, len(rx_sink.received))

    def test_2(self):
        sink = TestObserver()
        rx_sink = TestRxObserver()
        source_multicast = TestMultiCast()
        reduce_multicast = ReduceMultiCast(ReduceMultiCast(source=source_multicast))
        reduce_multicast.get_source(self.info).subscribe(rx_sink)
        source_flowable1 = TestFlowable()
        source_flowable2 = TestFlowable()
        source_multicast.on_next(Flowable(source_flowable1))
        source_multicast.on_next(Flowable(source_flowable2))
        source_multicast.on_completed()

        subscription = rx_sink.received[0].unsafe_subscribe(Subscriber(
            scheduler=self.source_scheduler,
            subscribe_scheduler=self.source_scheduler
        ))
        subscription.observable.observe(ObserverInfo(observer=sink))
        self.multicast_scheduler.advance_by(1)
        source_flowable1.on_next_single(1)
        source_flowable2.on_next_single('a')
        source_flowable1.on_next_single(2)
        source_flowable1.on_completed()
        source_flowable2.on_next_single('b')
        source_flowable2.on_completed()

        self.assertEqual([1, 'a', 2, 'b'], sink.received)
        self.assertTrue(sink.is_completed)
