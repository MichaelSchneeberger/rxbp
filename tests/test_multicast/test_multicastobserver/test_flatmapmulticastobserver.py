import threading
import unittest

from rx.disposable import CompositeDisposable

from rxbp.acknowledgement.continueack import ContinueAck
from rxbp.init.initobserverinfo import init_observer_info
from rxbp.multicast.init.initmulticastobserverinfo import init_multicast_observer_info
from rxbp.multicast.multicastobserverinfo import MultiCastObserverInfo
from rxbp.multicast.multicastobservers.flatmapmulticastobserver import FlatMapMultiCastObserver
from rxbp.multicast.testing.testmulticastobservable import TestMultiCastObservable
from rxbp.multicast.testing.testmulticastobserver import TestMultiCastObserver
from rxbp.observers.flatmapobserver import FlatMapObserver
from rxbp.states.rawstates.rawflatmapstates import RawFlatMapStates
from rxbp.testing.tobservable import TObservable
from rxbp.testing.tobserver import TObserver
from rxbp.testing.tscheduler import TScheduler


class TestFlatMapMultiCastObserver(unittest.TestCase):
    def setUp(self):
        self.source = TestMultiCastObservable()
        self.inner_source_1 = TestMultiCastObservable()
        self.inner_source_2 = TestMultiCastObservable()

        self.sink = TestMultiCastObserver()
        self.scheduler = TScheduler()
        self.composite_disposable = CompositeDisposable()
        self.exc = Exception()
        self.lock = threading.RLock()

    def test_initialize(self):
        observer = FlatMapMultiCastObserver(
            observer_info=init_multicast_observer_info(observer=self.sink),
            composite_disposable=self.composite_disposable,
            func=lambda v: v,
            lock=self.lock,
            state=[1],
        )

    def test_send_inner_observable(self):
        observer = FlatMapMultiCastObserver(
            observer_info=init_multicast_observer_info(observer=self.sink),
            composite_disposable=self.composite_disposable,
            func=lambda v: v,
            lock=self.lock,
            state=[1],
        )
        self.source.observe(init_multicast_observer_info(observer))

        self.source.on_next_single(self.inner_source_1)

    def test_send_inner_items(self):
        observer = FlatMapMultiCastObserver(
            observer_info=init_multicast_observer_info(observer=self.sink),
            composite_disposable=self.composite_disposable,
            func=lambda v: v,
            lock=self.lock,
            state=[1],
        )
        self.source.observe(init_multicast_observer_info(observer))
        self.source.on_next_single(self.inner_source_1)
        self.inner_source_1.on_next_single(1)
        self.inner_source_1.on_next_single(2)

        self.assertListEqual(self.sink.received, [1, 2])

    def test_two_observables(self):
        observer = FlatMapMultiCastObserver(
            observer_info=init_multicast_observer_info(observer=self.sink),
            composite_disposable=self.composite_disposable,
            func=lambda v: v,
            lock=self.lock,
            state=[1],
        )
        self.source.observe(init_multicast_observer_info(observer))
        self.source.on_next_single(self.inner_source_1)
        self.inner_source_1.on_next_single(1)
        self.inner_source_1.on_next_single(2)

        self.source.on_next_single(self.inner_source_2)
        self.inner_source_2.on_next_single(3)
        self.inner_source_2.on_completed()

        self.inner_source_1.on_next_single(4)
        self.inner_source_1.on_completed()

        self.source.on_completed()

        self.assertListEqual(self.sink.received, [1, 2, 3, 4])
        self.assertTrue(self.sink.is_completed)
