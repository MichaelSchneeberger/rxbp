import unittest

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
        self.source_multicast = TestMultiCast()
        self.rx_sink = TestRxObserver()
        self.source1 = TestFlowable()
        self.source2 = TestFlowable()

    def test_send_single_flowable(self):
        reduce_multicast = ReduceMultiCast(source=self.source_multicast)
        reduce_multicast.get_source(self.info).subscribe(self.rx_sink)

        self.source_multicast.on_next(Flowable(self.source1))

        self.assertEqual(1, len(self.rx_sink.received))

    def test_send_dictionary(self):
        reduce_multicast = ReduceMultiCast(source=self.source_multicast)
        reduce_multicast.get_source(self.info).subscribe(self.rx_sink)

        self.source_multicast.on_next({'f1': Flowable(self.source1)})

        self.assertEqual(1, len(self.rx_sink.received))

    def test_reduce_single_flowables_without_maintaining_order(self):
        reduce_multicast = ReduceMultiCast(source=self.source_multicast)
        reduce_multicast.get_source(self.info).subscribe(self.rx_sink)
        self.source_multicast.on_next(Flowable(self.source1))
        self.source_multicast.on_next(Flowable(self.source2))
        self.source_multicast.on_completed()

        sink = TestObserver()
        subscription = self.rx_sink.received[0].unsafe_subscribe(Subscriber(
            scheduler=self.source_scheduler,
            subscribe_scheduler=self.source_scheduler
        ))
        subscription.observable.observe(ObserverInfo(observer=sink))

        # sending the lifted flowable is scheduled on the multicast_scheduler
        self.multicast_scheduler.advance_by(1)

        self.source1.on_next_single(1)
        self.source2.on_next_single('a')
        self.source1.on_next_single(2)
        self.source1.on_completed()
        self.source2.on_next_single('b')
        self.source2.on_completed()

        self.assertEqual([1, 'a', 2, 'b'], sink.received)
        self.assertTrue(sink.is_completed)

    def test_reduce_single_flowables_with_maintaining_order(self):
        reduce_multicast = ReduceMultiCast(
            source=self.source_multicast,
            maintain_order=True,
        )
        reduce_multicast.get_source(self.info).subscribe(self.rx_sink)
        self.source_multicast.on_next(Flowable(self.source1))
        self.source_multicast.on_next(Flowable(self.source2))
        self.source_multicast.on_completed()

        sink = TestObserver()
        subscription = self.rx_sink.received[0].unsafe_subscribe(Subscriber(
            scheduler=self.source_scheduler,
            subscribe_scheduler=self.source_scheduler
        ))
        subscription.observable.observe(ObserverInfo(observer=sink))

        # sending the lifted flowable is scheduled on the multicast_scheduler
        self.multicast_scheduler.advance_by(1)

        self.source1.on_next_single(1)
        self.source2.on_next_single('a')
        self.source1.on_next_single(2)
        self.source1.on_completed()
        self.source2.on_next_single('b')
        self.source2.on_completed()

        self.assertEqual([1, 2, 'a', 'b'], sink.received)
        self.assertTrue(sink.is_completed)
